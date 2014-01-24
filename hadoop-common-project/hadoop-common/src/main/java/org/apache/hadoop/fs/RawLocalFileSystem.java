/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileDescriptor;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.rmi.dgc.VMID;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * Implement the FileSystem API for the raw local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RawLocalFileSystem extends FileSystem {
  static final URI NAME = URI.create("file:///");
  private Path workingDir;
  private boolean isPrefetchEnabled;
  private boolean isPrefetchInputStreamEnabled;
  private String prefetchDirPath;
  
  public RawLocalFileSystem() {
    workingDir = getInitialWorkingDirectory();
  }
  
  private Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new Path(workingDir, f);
    }
  }
  
  /** Convert a path to a File. */
  public File pathToFile(Path path) {
    checkPath(path);
    if (!path.isAbsolute()) {
      path = new Path(getWorkingDirectory(), path);
    }
    return new File(path.toUri().getPath());
  }

  @Override
  public URI getUri() { return NAME; }
  
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
    isPrefetchEnabled = getConf().getBoolean("yarn.inmemory.enabled", false);
    isPrefetchInputStreamEnabled = getConf().getBoolean("yarn.inmemory.prefetch.inputstream.enabled", false);
    prefetchDirPath = getConf().get("yarn.inmemory.prefetch.dir", "/dev/shm/hadoop");
    LOG.error("@@ FS: prefetch enabled=" + isPrefetchEnabled + ", prefetch inputstream enabled=" + isPrefetchInputStreamEnabled + ", prefetch dir=" + prefetchDirPath);
  }
  
  class TrackingFileInputStream extends FileInputStream {
    public TrackingFileInputStream(File f) throws IOException {
      super(f);
    }
    
    @Override
    public int read() throws IOException {
      int result = super.read();
      if (result != -1) {
        statistics.incrementBytesRead(1);
      }
      return result;
    }
    
    @Override
    public int read(byte[] data) throws IOException {
      int result = super.read(data);
      if (result != -1) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }
    
    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      int result = super.read(data, offset, length);
      if (result != -1) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }
  }
  
 

  /*******************************************************
   * For open()'s FSInputStream.
   *******************************************************/
  class LocalFSFileInputStream extends FSInputStream implements HasFileDescriptor {
    private FileInputStream fis;
    private long position;

    public LocalFSFileInputStream(Path f) throws IOException {
      this.fis = new TrackingFileInputStream(pathToFile(f));
    }
    
    @Override
    public void seek(long pos) throws IOException {
      fis.getChannel().position(pos);
      this.position = pos;
    }
    
    @Override
    public long getPos() throws IOException {
      return this.position;
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
    
    /*
     * Just forward to the fis
     */
    @Override
    public int available() throws IOException { return fis.available(); }
    @Override
    public void close() throws IOException { fis.close(); }
    @Override
    public boolean markSupported() { return false; }
    
    @Override
    public int read() throws IOException {
      try {
        int value = fis.read();
        if (value >= 0) {
          this.position++;
        }
        return value;
      } catch (IOException e) {                 // unexpected exception
        throw new FSError(e);                   // assume native fs error
      }
    }
    
    long totalTime = 0;
    long totalRead = 0;
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
    	    LOG.error("@@ FS: going to read -> buf=" + b.length + ",off=" + off + ", len=" + len);
    	    long startTime = System.nanoTime();
        int value = fis.read(b, off, len);
        long ioTime = System.nanoTime() - startTime;
        LOG.error("@@ FS: finish to read");
        if (value > 0) {
          this.position += value;
        }
        totalTime += ioTime;
        totalRead+=value;
        LOG.error("@_@ NFS IO time=" + ioTime + ", total=" + totalTime);
        return value;
      } catch (IOException e) {                 // unexpected exception
        throw new FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public int read(long position, byte[] b, int off, int len)
      throws IOException {
      ByteBuffer bb = ByteBuffer.wrap(b, off, len);
      try {
        return fis.getChannel().read(bb, position);
      } catch (IOException e) {
        throw new FSError(e);
      }
    }
    
    @Override
    public long skip(long n) throws IOException {
      long value = fis.skip(n);
      if (value > 0) {
        this.position += value;
      }
      return value;
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
      return fis.getFD();
    }
  }
  
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (!exists(f)) {
      throw new FileNotFoundException(f.toString());
    }
    if (isPrefetchEnabled && isPrefetchInputStreamEnabled && isPrefetchPath(f.toUri().getPath())) {
    		LOG.error("@@ FS: open prefetch input stream=" + f);
    		return new FSDataInputStream(new BufferedFSInputStream(new PrefetchedFileInputStream(f), bufferSize));
    }
    LOG.error("@@ FS: open local input stream=" + f);
    return new FSDataInputStream(new BufferedFSInputStream(
        new LocalFSFileInputStream(f), bufferSize));
  }
  
  private boolean isPrefetchPath(String path) {
//	  	  return false;
	  //return path.contains("nfs") && !path.contains("split") && !path.contains("crc") && !path.contains("staging")?true:false;
	  return path.contains("nfs") && !path.contains("split") && !path.contains("crc") && !path.contains("staging") && ((path.contains("bgwiki") || path.contains("file")) || !path.contains("."))?true:false;
  }
  
  class 
  PrefetchedFileInputStream extends FSInputStream implements HasFileDescriptor {
	    private FileInputStream fis;
	    private long position;
	    private File currentPrefetchFile;
	    private FileInputStream currentFis;
	    private int currentBlockIndex;
	    private RandomAccessFile currentProgress;
	    private long nextReadBoundary;
	    private int blockSize = 64*1024*1024;
	    private File prefetchDir = new File(prefetchDirPath);
	    private Path f;
	    private long blockReadCount;
	    private MappedByteBuffer currentRecord;
	    private FileChannel currentFileChannel;
	    private int maxBlockIndex;
	    private int fileSize;
	    private boolean isPrefetchStream;
	    
	    private VMID vmid = new VMID();
	    
	    public PrefetchedFileInputStream(Path f) throws IOException {
	      this.fis = new TrackingFileInputStream(pathToFile(f));
	      this.currentFis = fis;
	      this.f = f;
	      this.currentBlockIndex = -1;
	      this.position = 0;
	      this.maxBlockIndex = this.fis.available() / blockSize - 1 + (this.fis.available() % blockSize > 0 ? 1 : 0);
	      this.fileSize = this.available();
	      switchPrefetchInputStream(0);
	    }
	    
	    private File convertPrefetchProgressFilePath(int blockIndex) {
    			File prefetchBlockFile = new File(prefetchDir, f.getName() + "." + blockSize + "."+(blockSize*blockIndex) + ".progress");
    			return prefetchBlockFile;
	    }
	    
	    private File convertPrefetchFilePath(int blockIndex) {
	    		File prefetchBlockFile = new File(prefetchDir, f.getName() + "." + blockSize + "."+(blockSize*blockIndex));
	    		return prefetchBlockFile;
	    }
	    
	    private boolean waitForFile(File file, int count, long delay) {
	    		int waitCount = 0;
	    		while (waitCount < count && !file.exists()) {
	    			try {
	    				LOG.error("[" + this.vmid + "] FS: waiting for file to present -> file=" + file.getPath());
	    				Thread.sleep(delay);
				} catch (InterruptedException e) {
					LOG.error("[" + this.vmid + "] FS: waiting failed -> file=" + file.getPath(), e);
				}
	    		}
	    		return file.exists();
	    	
	    }
	    
	    private void switchPrefetchInputStream(long pos) throws IOException  {
	    		int blockIndex = (int) (pos/blockSize);
	    		LOG.error("[" + this.vmid + "] FS: blockIndex=" + blockIndex + ", currentBlockIndex="+ this.currentBlockIndex + ", position=" + pos + ", available=" + this.fis.available());
	    		if (pos >= this.fileSize) {
	    			switchToLocalInputStream();
	    			// TODO: should this be only the file size?
	    			this.nextReadBoundary = this.fileSize;
	    		} else {
		    		if (blockIndex != this.currentBlockIndex) {
		    			File prefetchBlockProgressFile = convertPrefetchProgressFilePath(blockIndex);
		    			File prefetchBlockFile = convertPrefetchFilePath(blockIndex);
		    			this.currentBlockIndex = blockIndex;
		    			this.blockReadCount = 0;
		    			
	    				LOG.error("[" + this.vmid + "] FS: prefetch block file=" + prefetchBlockFile.exists() + ", progress file=" + prefetchBlockProgressFile.exists());
		    			//if (waitForFile(prefetchBlockProgressFile, 10, 100) && waitForFile(prefetchBlockProgressFile, 10, 100)) {
		    			if (prefetchBlockFile.exists() && prefetchBlockProgressFile.exists())
		    			{
		    				LOG.error("[" + this.vmid + "] FS: switch to prefetch inputstream -> " + prefetchBlockFile.getPath());
		    				this.currentPrefetchFile = prefetchBlockFile;
		    				this.currentFis = new FileInputStream(prefetchBlockFile);
		    				this.currentProgress = new RandomAccessFile(prefetchBlockProgressFile, "r");
		    				//TODO: block size must be equal to the prefetch file size
		    				//this.currentFileChannel = new FileInputStream(prefetchBlockFile).getChannel();
		    				//this.currentRecord = this.currentFileChannel.map(MapMode.READ_ONLY, 0, blockSize);
		    				isPrefetchStream = true;
		    			} else {
		    				switchToLocalInputStream();
		    			}
		    			this.nextReadBoundary = (this.currentBlockIndex+1) * blockSize;
		    		}
	    		}
	    }
	    
	    private void switchToLocalInputStream() {
	    	    LOG.error("[" + this.vmid + "] FS: switch to local inputstream");
			this.currentPrefetchFile = null;
			this.currentFis = this.fis;
			this.currentProgress = null;
			this.currentRecord = null;
			this.currentFileChannel = null;
			this.isPrefetchStream = false;
	    }
	    
	    private int getBlockProgress() {
	    		try {
		    		this.currentProgress.seek(0);
		    		int value = this.currentProgress.readInt();
		    		return value;
	    		} catch (Exception e) {
		    		LOG.error("[" + this.vmid + "] progress file is not ready", e);
		    	}
		    	return -1;
	    }
	    
	    long progressTotalTime = 0;
	    long switchTotalTime = 0;
	    long realTotalTime = 0;
	    private int readFromPrefetching(byte[] b, int off, int len) throws IOException {
	    		long switchStartTime = System.nanoTime();
	    		switchPrefetchInputStream(getPos());
	    		long switchTime = System.nanoTime() - switchStartTime;
	    		switchTotalTime += switchTime;
	    		LOG.error("@@ Switch IO time=" + switchTime + ", total=" + switchTotalTime);
	    		if (this.isPrefetchStream) {
	    			long progressStartTime = System.nanoTime();
	    			int progress = getBlockProgress();
	    			long progressTime = System.nanoTime() - progressStartTime;
	    			LOG.error("@@ Progress IO time=" + progressTime + ", total=" + progressTotalTime);
	    			int progressPosition = this.currentBlockIndex * blockSize + progress;
	    			if (getPos() + len <= progressPosition) {
	    				long realStartTime = System.nanoTime();
	    				int value = this.currentFis.read(b, off, len);
	    				//ByteBuffer bb = ByteBuffer.wrap(b, off, len);
			    		//int value = this.currentFileChannel.read(bb);
	    				long realTime = System.nanoTime() - realStartTime;
	    				realTotalTime += realTime;
	    				LOG.error("@@ Real prefetching IO time=" + realTime + ", total=" + realTotalTime);
			    		return value;
	    			} 
	    		} 
	    		return Integer.MIN_VALUE;
	    			/**
	    			int nRead = 0;
		    		int value = 0;
		    		while (nRead<len && value != -1) {
		    			switchPrefetchInputStream(getPos());
	    			int remainging = len-nRead;
	    			int readLen = (int) (getPos() + remainging < this.nextReadBoundary? remainging : (this.nextReadBoundary - getPos()));
	    			int readOffset = off + nRead;
	    			LOG.error("[" + this.vmid + "] FS: nRead=" + nRead + ", remaining=" + remainging + ", readOffset=" + readOffset + ", readLen=" + readLen + ", position=" + getPos());
	    			// this block is prefetched
	    			if (this.isPrefetchStream) {
	    				int progress = -1;
	    				// TODO: problemetic
	    				int readTries = 0;
	    				int maxReadTries = 10;
	    				while ( readTries++ < maxReadTries && ((progress = getBlockProgress()) == -1 ) && (progress < (this.blockReadCount + readLen)) ) {
	    					try {
	    							LOG.error("[" + this.vmid + "] FS: read tries=" + readTries);
//	    							LOG.error("@@ FS: blockReadCount=" + this.blockReadCount + ", nexReadPosition=" + (this.blockReadCount + readLen) + ", progress=" + progress);
//	    							LOG.error("@@ FS: prefetching data is not ready");
								Thread.sleep(10);
							} catch (InterruptedException e) {
								LOG.error("[" + Thread.currentThread().getId() + "] FS: failure happened while waiting", e);
							}
	    				}
	    				if (progress == -1) {
	    					throw new IOException("Progress file is not ready");
	    				}
	    				LOG.error("[" + this.vmid + "] FS: blockReadCount=" + this.blockReadCount + ", nexReadPosition=" + (this.blockReadCount + readLen) + ", progress=" + progress);
		    			LOG.error("[" + this.vmid + "] FS: pread => pos=" + getPos() + ", block=" + this.currentBlockIndex + ", offset=" + off + ", len=" + len + ", remaining=" + remainging);
		    			LOG.error("[" + this.vmid + "] FS: pread => bLength=" + b.length + ", bOffset=" + (off + nRead) +  ", bReadLen=" + readLen + ", nextRead=" + (getPos() + readLen) + ", boundary=" + nextReadBoundary + ", remaining=" + this.currentFileChannel.size());
		    			ByteBuffer bb = ByteBuffer.wrap(b, readOffset, readLen);
		    			value = this.currentFileChannel.read(bb);
	    				//this.currentRecord.get(b, readOffset, readLen);
		    			LOG.error("[" + this.vmid + "] FS: pread => value=" + value);
		    			if (value > 0) {
		    				this.blockReadCount += value;
		    				nRead += value;
		    			} else {
		    				LOG.equals("[" + this.vmid + "] FS: reached EOF");
		    				break;
		    			}
	    			} else {
	    				throw new IOException("[" + this.vmid + "] The block is not prefetched yet -> position=" + getPos() + ", length=" + len);
	    			}
	    			
	    		}
	    		return nRead;
	    		*/
	    }
	    
	    @Override
	    public void seek(long pos) throws IOException {
	      fis.getChannel().position(pos);
	      this.position = pos;
	    }
	    
	    @Override
	    public long getPos() throws IOException {
	      return this.position;
	    }
	    
	    @Override
	    public boolean seekToNewSource(long targetPos) throws IOException {
	      return false;
	    }
	    
	    /*
	     * Just forward to the fis
	     */
	    @Override
	    public int available() throws IOException { return fis.available(); }
	    @Override
	    public void close() throws IOException { fis.close(); }
	    @Override
	    public boolean markSupported() { return false; }
	    
	    @Override
	    public int read() throws IOException {
	      try {
	    	  	// TODO: need to read from prefetching input stream
	    	  	return this.fis.read();
	      } catch (IOException e) {                 // unexpected exception
	        throw new FSError(e);                   // assume native fs error
	      }
	    }
	    long totalRead = 0;
	    long totalTime = 0;
	    @Override
	    public int read(byte[] b, int off, int len) throws IOException {
	      try {
	    	  	int value;
	    	    LOG.error("@@ FS: going to read -> buf=" + b.length + ",off=" + off + ", len=" + len);
	    	    long localStartTime = System.nanoTime();
	    	  	value = readFromPrefetching(b, off, len);
	    	  	long localTime = System.nanoTime() - localStartTime;
		    totalTime += localTime;
		    totalRead+=value;
		    LOG.error("@_@ Prefetching IO time=" + localTime + ", total=" + totalTime);
	    	  	// LOG.error("@@ FS: real read=" + value);
		    if (value > 0) {
		        this.position += value;
		        skip(value);
		        return value;
		    } 
	    	    long remoteStartTime = System.nanoTime();
	    	  	value = fis.read(b, off, len);
	    	  	long remoteTime = System.nanoTime() - remoteStartTime;
	        if (value > 0) {
	          this.position += value;
	        }
	        totalTime += remoteTime;
	        totalRead += value;
	        LOG.error("@_@ Prefetching IO time=" + remoteTime + ", total=" + totalTime);
	        return value;
	      } catch (IOException e) {                 // unexpected exception
	        throw new FSError(e);                   // assume native fs error
	      }
	    }
	    
	    @Override
	    public int read(long position, byte[] b, int off, int len)
	      throws IOException {
	      try {
	    	  		seek(position);
	    	  		int value = read(b, off, len);
		        if (value > 0) {
		          this.position += value;
		        }
		        return value;
	      } catch (IOException e) {
	        throw new FSError(e);
	      }
	    }
	    
	    @Override
	    public long skip(long n) throws IOException {
	      long value = fis.skip(n);
	      if (value > 0) {
	        this.position += value;
	      }
	      return value;
	    }

	    @Override
	    public FileDescriptor getFileDescriptor() throws IOException {
	      return fis.getFD();
	    }
	  }
	  
  
  /*********************************************************
   * For create()'s FSOutputStream.
   *********************************************************/
  class LocalFSFileOutputStream extends OutputStream {
    private FileOutputStream fos;
    
    private LocalFSFileOutputStream(Path f, boolean append) throws IOException {
      this.fos = new FileOutputStream(pathToFile(f), append);
    }
    
    /*
     * Just forward to the fos
     */
    @Override
    public void close() throws IOException { fos.close(); }
    @Override
    public void flush() throws IOException { fos.flush(); }
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      try {
        fos.write(b, off, len);
      } catch (IOException e) {                // unexpected exception
        throw new FSError(e);                  // assume native fs error
      }
    }
    
    @Override
    public void write(int b) throws IOException {
      try {
        fos.write(b);
      } catch (IOException e) {              // unexpected exception
        throw new FSError(e);                // assume native fs error
      }
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    if (!exists(f)) {
      throw new FileNotFoundException("File " + f + " not found");
    }
    if (getFileStatus(f).isDirectory()) {
      throw new IOException("Cannot append to a diretory (=" + f + " )");
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        new LocalFSFileOutputStream(f, true), bufferSize), statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
    short replication, long blockSize, Progressable progress)
    throws IOException {
    return create(f, overwrite, true, bufferSize, replication, blockSize, progress);
  }

  private FSDataOutputStream create(Path f, boolean overwrite,
      boolean createParent, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (exists(f) && !overwrite) {
      throw new IOException("File already exists: "+f);
    }
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent.toString());
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        new LocalFSFileOutputStream(f, false), bufferSize), statistics);
  }
  
  @Override
  @Deprecated
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
      throw new IOException("File already exists: "+f);
    }
    return new FSDataOutputStream(new BufferedOutputStream(
        new LocalFSFileOutputStream(f, false), bufferSize), statistics);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite, int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {

    FSDataOutputStream out = create(f,
        overwrite, bufferSize, replication, blockSize, progress);
    setPermission(f, permission);
    return out;
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    FSDataOutputStream out = create(f,
        overwrite, false, bufferSize, replication, blockSize, progress);
    setPermission(f, permission);
    return out;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    if (pathToFile(src).renameTo(pathToFile(dst))) {
      return true;
    }
    return FileUtil.copy(this, src, this, dst, true, getConf());
  }
  
  /**
   * Delete the given path to a file or directory.
   * @param p the path to delete
   * @param recursive to delete sub-directories
   * @return true if the file or directory and all its contents were deleted
   * @throws IOException if p is non-empty and recursive is false 
   */
  @Override
  public boolean delete(Path p, boolean recursive) throws IOException {
    File f = pathToFile(p);
    if (f.isFile()) {
      return f.delete();
    } else if (!recursive && f.isDirectory() && 
        (FileUtil.listFiles(f).length != 0)) {
      throw new IOException("Directory " + f.toString() + " is not empty");
    }
    return FileUtil.fullyDelete(f);
  }
 
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    File localf = pathToFile(f);
    FileStatus[] results;

    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }
    if (localf.isFile()) {
      return new FileStatus[] {
        new RawLocalFileStatus(localf, getDefaultBlockSize(f), this) };
    }

    File[] names = localf.listFiles();
    if (names == null) {
      return null;
    }
    results = new FileStatus[names.length];
    int j = 0;
    for (int i = 0; i < names.length; i++) {
      try {
        results[j] = getFileStatus(new Path(names[i].getAbsolutePath()));
        j++;
      } catch (FileNotFoundException e) {
        // ignore the files not found since the dir list may have have changed
        // since the names[] list was generated.
      }
    }
    if (j == names.length) {
      return results;
    }
    return Arrays.copyOf(results, j);
  }

  /**
   * Creates the specified directory hierarchy. Does not
   * treat existence as an error.
   */
  @Override
  public boolean mkdirs(Path f) throws IOException {
    if(f == null) {
      throw new IllegalArgumentException("mkdirs path arg is null");
    }
    Path parent = f.getParent();
    File p2f = pathToFile(f);
    if(parent != null) {
      File parent2f = pathToFile(parent);
      if(parent2f != null && parent2f.exists() && !parent2f.isDirectory()) {
        throw new FileAlreadyExistsException("Parent path is not a directory: " 
            + parent);
      }
    }
    return (parent == null || mkdirs(parent)) &&
      (p2f.mkdir() || p2f.isDirectory());
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    boolean b = mkdirs(f);
    if(b) {
      setPermission(f, permission);
    }
    return b;
  }
  

  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
    throws IOException {
    boolean b = mkdirs(f);
    setPermission(f, absolutePermission);
    return b;
  }
  
  
  @Override
  public Path getHomeDirectory() {
    return this.makeQualified(new Path(System.getProperty("user.home")));
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
    checkPath(workingDir);
    
  }
  
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }
  
  @Override
  protected Path getInitialWorkingDirectory() {
    return this.makeQualified(new Path(System.getProperty("user.dir")));
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    File partition = pathToFile(p == null ? new Path("/") : p);
    //File provides getUsableSpace() and getFreeSpace()
    //File provides no API to obtain used space, assume used = total - free
    return new FsStatus(partition.getTotalSpace(), 
      partition.getTotalSpace() - partition.getFreeSpace(),
      partition.getFreeSpace());
  }
  
  // In the case of the local filesystem, we can just rename the file.
  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    rename(src, dst);
  }
  
  // We can write output directly to the final location
  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return fsOutputFile;
  }
  
  // It's in the right place - nothing to do.
  @Override
  public void completeLocalOutput(Path fsWorkingFile, Path tmpLocalFile)
    throws IOException {
  }
  
  @Override
  public void close() throws IOException {
    super.close();
  }
  
  @Override
  public String toString() {
    return "LocalFS";
  }
  
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    File path = pathToFile(f);
    if (path.exists()) {
      return new RawLocalFileStatus(pathToFile(f), getDefaultBlockSize(f), this);
    } else {
      throw new FileNotFoundException("File " + f + " does not exist");
    }
  }

  static class RawLocalFileStatus extends FileStatus {
    /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
     * We recognize if the information is already loaded by check if
     * onwer.equals("").
     */
    private boolean isPermissionLoaded() {
      return !super.getOwner().equals(""); 
    }
    
    RawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs) {
      super(f.length(), f.isDirectory(), 1, defaultBlockSize,
            f.lastModified(), fs.makeQualified(new Path(f.getPath())));
    }
    
    @Override
    public FsPermission getPermission() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getPermission();
    }

    @Override
    public String getOwner() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getOwner();
    }

    @Override
    public String getGroup() {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getGroup();
    }

    /// loads permissions, owner, and group from `ls -ld`
    private void loadPermissionInfo() {
      IOException e = null;
      try {
        StringTokenizer t = new StringTokenizer(
            execCommand(new File(getPath().toUri()), 
                        Shell.getGET_PERMISSION_COMMAND()));
        //expected format
        //-rw-------    1 username groupname ...
        String permission = t.nextToken();
        if (permission.length() > 10) { //files with ACLs might have a '+'
          permission = permission.substring(0, 10);
        }
        setPermission(FsPermission.valueOf(permission));
        t.nextToken();
        setOwner(t.nextToken());
        setGroup(t.nextToken());
      } catch (Shell.ExitCodeException ioe) {
        if (ioe.getExitCode() != 1) {
          e = ioe;
        } else {
          setPermission(null);
          setOwner(null);
          setGroup(null);
        }
      } catch (IOException ioe) {
        e = ioe;
      } finally {
        if (e != null) {
          throw new RuntimeException("Error while running command to get " +
                                     "file permissions : " + 
                                     StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      super.write(out);
    }
  }

  /**
   * Use the command chown to set owner.
   */
  @Override
  public void setOwner(Path p, String username, String groupname)
    throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }

    if (username == null) {
      execCommand(pathToFile(p), Shell.SET_GROUP_COMMAND, groupname); 
    } else {
      //OWNER[:[GROUP]]
      String s = username + (groupname == null? "": ":" + groupname);
      execCommand(pathToFile(p), Shell.SET_OWNER_COMMAND, s);
    }
  }

  /**
   * Use the command chmod to set permission.
   */
  @Override
  public void setPermission(Path p, FsPermission permission)
    throws IOException {
    if (NativeIO.isAvailable()) {
      NativeIO.chmod(pathToFile(p).getCanonicalPath(),
                     permission.toShort());
    } else {
      execCommand(pathToFile(p), Shell.SET_PERMISSION_COMMAND,
          String.format("%05o", permission.toShort()));
    }
  }

  private static String execCommand(File f, String... cmd) throws IOException {
    String[] args = new String[cmd.length + 1];
    System.arraycopy(cmd, 0, args, 0, cmd.length);
    args[cmd.length] = FileUtil.makeShellPath(f, true);
    String output = Shell.execCommand(args);
    return output;
  }

}