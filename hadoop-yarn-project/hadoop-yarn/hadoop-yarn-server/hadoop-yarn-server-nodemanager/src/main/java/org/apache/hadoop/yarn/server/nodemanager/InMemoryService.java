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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PrefetchInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.CompositeService;

/**
 * The class which provides functionality of checking the health of the node and
 * reporting back to the service for which the health checker has been asked to
 * report.
 */
public class InMemoryService extends CompositeService {

	private static final Log LOG = LogFactory.getLog(InMemoryService.class);
	private Context context;
	private File prefetchDir;

	private Timer timer;
	private ExecutorService prefetchService;
	private NetworkMonitor networkMonitor;

	// prefetching task queue
	private LinkedList<PrefetchInfo> prefetchQueue = new LinkedList<PrefetchInfo>();
	// task in prefetching
	private List<PrefetchInfo> prefetchingList = new LinkedList<PrefetchInfo>();
	// task completed prefetching
	private List<PrefetchInfo> completedPrefetchList = new LinkedList<PrefetchInfo>();
	
	// record to be pulled
	private List<PrefetchInfo> completedPrefetchRecord = new LinkedList<PrefetchInfo>();

	private Map<PrefetchInfo, PrefetchWorker> workers = new HashMap<PrefetchInfo, InMemoryService.PrefetchWorker>();

	private Queue<Token> tokenQueue = new LinkedList<InMemoryService.Token>();

	private int prefetchWindow;
	private int prefetchConcurrency;
	private boolean prefetchTransfer;

	private long blockSize;
	private int tokenNumber;
	private int tokenSize;
	private long tokenInterval;

	public InMemoryService(Context context, int prefetchWindow, int concurrency, boolean transfer) {
		super(InMemoryService.class.getName());
		this.context = context;
		this.prefetchWindow = prefetchWindow;
		this.prefetchConcurrency = concurrency;
		this.prefetchTransfer = transfer;
		prefetchService = Executors.newFixedThreadPool(prefetchConcurrency);
		networkMonitor = new NetworkMonitor();
		timer = new Timer();
	}

	@Override
	public void init(Configuration conf) {
		super.init(conf);
		LOG.error("@@ IMS: initialize");
		prefetchDir = new File("/dev/shm/hadoop");
		prefetchDir.mkdirs();
		networkMonitor.init();
		blockSize = conf.getInt("fs.local.block.size", 64 * 2014 * 1024);
		LOG.error("@@ IMS: block size=" + blockSize);
		tokenNumber = conf.getInt(YarnConfiguration.IM_TOKEN_NUMBER, YarnConfiguration.DEFAULT_IM_TOKEN_NUMBER);
		tokenSize = conf.getInt(YarnConfiguration.IM_TOKEN_SIZE, YarnConfiguration.DEFAULT_IM_TOKEN_SIZE);
		tokenInterval = conf.getLong(YarnConfiguration.IM_TOKEN_INTERVAL, YarnConfiguration.DEFAULT_IM_TOKEN_INTERVAL);
		LOG.error("@@ IMS: token -> number=" + tokenNumber + ", size=" + tokenSize + ", interval=" + tokenInterval);
	}

	@Override
	public synchronized void start() {
		LOG.error("@@ IMS: start service");
		super.start();
		networkMonitor.start();
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				dispatchPrefetchTask();
			}
		}, 1000, 1000);
	}

	@Override
	public synchronized void stop() {
		super.stop();
		networkMonitor.stop();
		/**
		 * for (File file : prefetchDir.listFiles()) { file.delete(); }
		 * prefetchDir.delete();
		 */
	}

	public List<PrefetchInfo> getPrefetchProgress() {
		LOG.error("@@ IMM: return prefetched splits");
		List<PrefetchInfo> completedTasks = new LinkedList<PrefetchInfo>(completedPrefetchRecord);
		completedPrefetchRecord.clear();
		return completedTasks;
	}

	public int getAvailableWindow() {
		return this.prefetchWindow - (prefetchingList.size() + completedPrefetchList.size());
	}

	public void dispatchPrefetchTask() {
		LOG.error("@@ IMS: dispatch prefetch tasks -> size=" + prefetchQueue.size() + ", window=" + getAvailableWindow() + ", prefewtchWindow=" + this.prefetchWindow + ", prefetchingNum="
				+ prefetchingList.size() + ", completedList=" + completedPrefetchRecord.size());
		if (prefetchQueue.size() < 1 || getAvailableWindow() < 1) {
			LOG.error("@@ IMS: cannot perform prefetching");
			return;
		}
		LOG.error("@@ IMS: has available window for prefetching");
		for (; getAvailableWindow() > 0 && prefetchQueue.size() > 0;) {
			PrefetchInfo pi = prefetchQueue.poll();
			prefetchingList.add(pi);
			if (this.prefetchTransfer) {
				LOG.error("@@ IMS: dispatch prefetching task -> " + pi);
				PrefetchWorker pw = new PrefetchWorker(pi);
				LOG.error("@@ IMS: ready to initialize prefetch worker -> task=" + pi.taskId);
				pw.init();
				prefetchService.submit(pw);
				workers.put(pi, pw);
			} else {
				reportPrefetchProgress(pi);
			}
		}

	}

	public void notifyStartedContainer(ContainerId containerId) {
		LOG.error("@@ IMS: container started -> " + containerId);
	}

	public void addPrefetchRequestToQueue(List<PrefetchInfo> newPrefetchTasks) {
		LOG.error("@@ IMS: add prefetch request -> size=" + newPrefetchTasks.size());
		for (PrefetchInfo pi : newPrefetchTasks) {
			LOG.error("@@ IMS: add prefetching task -> " + pi);
			pi.nodeId = context.getNodeId().getHost();
			pi.progress = 0;
			prefetchQueue.offer(pi);
		}
	}

	public void revokePrefetchTasks(List<PrefetchInfo> prefetchTasks) {
		LOG.error("@@ IMS: revoke prefetch tasks -> num=" + prefetchTasks.size());
		for (PrefetchInfo pi : prefetchTasks) {
			revokePrefetchTask(pi);
		}
	}

	public void revokePrefetchTask(PrefetchInfo pi) {
		LOG.error("IMS: revoke prefetch task: node=" + this.context.getNodeId() + ", task=" + pi.taskId);
		// TODO: make sure these are the only things to delete
		// Have to delete in real system
		//File prefetchFile = getPrefetchFile(pi);
		//LOG.error("IMS: delete prefetch file -> file=" + prefetchFile.getPath() + ", delte=" + prefetchFile.delete());
		completedPrefetchList.remove(pi);
		prefetchingList.remove(pi);
	}

	// TODO: support only finished split now
	public void reportPrefetchProgress(PrefetchInfo pi) {
		LOG.error("@@ IMS: report prefetching split -> " + pi);
		pi.progress = pi.fileLength;
		completedPrefetchRecord.add(pi);
	}

	public String convertFilePath(String filePath) {
		return filePath.replace("file:", "");
	}

	public File getPrefetchProgressFile(PrefetchInfo pi) {
		String filePath = convertFilePath(pi.file);
		File inputFile = new File(filePath);
		File prefetchOutputFile = new File(this.prefetchDir, inputFile.getName() + "." + pi.fileLength + "." + pi.fileOffset + ".progress");
		return prefetchOutputFile;
	}

	public File getPrefetchFile(PrefetchInfo pi) {
		String filePath = convertFilePath(pi.file);
		File inputFile = new File(filePath);
		File prefetchOutputFile = new File(this.prefetchDir, inputFile.getName() + "." + pi.fileLength + "." + pi.fileOffset);
		return prefetchOutputFile;
	}

	public synchronized Token getDownloadToken(PrefetchInfo pi) {
		Token token = null;
		while ((token = tokenQueue.poll()) == null) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				LOG.error("@@ IMS: has problem to get token -> task=" + pi.taskId, e);
			}
		}
		return token;
	}

	class Token {
		int num;
		int unit;
		int value;

		public Token(int num, int unit) {
			this.num = num;
			this.unit = unit;
			this.value = this.num * this.unit;
		}

		public boolean hasAvailableValue() {
			return this.value > 0;
		}

		public void use(int usage) {
			this.value -= usage;
		}

	}

	class PrefetchWorker extends Thread {

		private PrefetchInfo split;
		private File progressFile;
		private File prefetchFile;
		private RandomAccessFile progressRecord;
		private RandomAccessFile prefetchRecord;
		private FileChannel prefetchFileChannel;

		int sizeOfByteBuffer = 8192;
		int sizeOfBuffer = 1024;

		public PrefetchWorker(PrefetchInfo split) {
			this.split = split;
		}

		public void init() {
			LOG.error("[" + this.split.taskId + "] PW: initializing...");
			this.progressFile = getPrefetchProgressFile(this.split);
			this.prefetchFile = getPrefetchFile(this.split);
			this.initializeProgressFile();
			this.initializePrefetchFile();
		}

		private void initializeProgressFile() {
			try {
				this.progressRecord = new RandomAccessFile(this.progressFile, "rw");
				this.progressRecord.setLength(0);
				LOG.error("[" + this.split.taskId + "] PW: created process file -> file=" + this.progressFile + ", exist=" + this.progressFile.exists());
			} catch (Exception e) {
				LOG.error("[" + this.split.taskId + "] PW: generating progress file failed -> " + this.split.taskId, e);
			}
		}

		private void initializePrefetchFile() {
			try {
				this.prefetchRecord = new RandomAccessFile(this.prefetchFile, "rw");
				this.prefetchRecord.setLength(blockSize);
				LOG.error("[" + this.split.taskId + "] PW: created prefetch file -> file=" + this.prefetchFile + ", exist=" + this.prefetchFile.exists());
			} catch (Exception e) {
				LOG.error("[" + this.split.taskId + "] PW: generating prefetch file failed -> " + this.split.taskId, e);
			}
		}

		private void updateProgressFile(int progress) {
			try {
				LOG.error("[" + this.split.taskId + "] PW: update progress file -> split=" + this.prefetchFile.getPath() + ", progress=" + progress);
				this.progressRecord.seek(0);
				this.progressRecord.writeInt(progress);
			} catch (IOException e) {
				LOG.error("[" + this.split.taskId + "] PW: update progress file failed -> " + this.split.taskId, e);
			}
		}

		@Override
		public void run() {
			try {
				LOG.error("[" + this.split.taskId + "] PW: start to prefetch");
				String filePath = convertFilePath(split.file);
				File inputFile = new File(filePath);
				FileInputStream fis = new FileInputStream(inputFile);
				FileChannel fc = fis.getChannel();
				fc.position(split.fileOffset);

				LOG.error("[" + this.split.taskId + "] PW: create mapped byte buffer, remaing=" + fc.size());
				LOG.error("[" + this.split.taskId + "] PW: start to prefetch -> task=" + split.taskId + ", read_path=" + inputFile.getPath() + ", write_path=" + this.prefetchFile.getPath());

				ByteBuffer bb = ByteBuffer.allocateDirect(sizeOfByteBuffer);
				byte[] barray = new byte[sizeOfBuffer];

				this.prefetchRecord.seek(0);

				int nRead, nGet;
				int prefetchCount = 0;
				long targetCount = fc.position() + blockSize < fc.size() ? blockSize : fc.size() - fc.position();
				boolean isEOF = false;
				long timeStart = System.nanoTime();
				while (prefetchCount < targetCount && !isEOF) {
					LOG.error("[" + this.split.taskId + "] PW: prefetch_count=" + prefetchCount + ", target_count=" + targetCount);
					Token token = getDownloadToken(this.split);
					LOG.error("[" + this.split.taskId + "] PW: get download token=" + token);
					while (token.hasAvailableValue()) {
						nRead = fc.read(bb);
						if (nRead == -1) {
							isEOF = true;
							break;
						} else if (nRead == 0) {
							continue;
						}
						token.use(nRead);
						bb.position(0);
						bb.limit(nRead);
						while (bb.hasRemaining()) {
							//LOG.error("[" + this.split.taskId + "] PW: remaining=" + bb.remaining());
							nGet = Math.min(bb.remaining(), sizeOfBuffer);
							bb.get(barray, 0, nGet);
							//LOG.error("[" + this.split.taskId + "] PW: read data to array -> " + this.split.taskId);
							this.prefetchRecord.write(barray, 0, nGet);
							//LOG.error("[" + this.split.taskId + "] PW: write array to file -> " + this.split.taskId);
							prefetchCount += nGet;
						}
						bb.clear();
						// update progreee per 8K read
						updateProgressFile(prefetchCount);
					}
				}
				long timeEnd = System.nanoTime();
				LOG.error("[" + this.split.taskId + "] PW: prefetching time=" + (timeEnd - timeStart) + ", amount=" + prefetchCount);

				fc.close();
				fis.close();
				
				this.prefetchRecord.setLength(prefetchCount);
				this.prefetchRecord.close();
				this.progressRecord.close();
				reportPrefetchProgress(split);
				LOG.error("[" + this.split.taskId + "] PW: finish to prefetch file: " + split);
			} catch (Exception ex) {
				LOG.error("[" + this.split.taskId + "] PW: read nfs file failed ->" + ex);
			}
		}
	}

	class NetworkMonitor {
		List<String> devices = new LinkedList<String>();
		double rxBytes, txBytes;
		double rxRate, txRate;
		double previousRxBytes, previousTxBytes;
		double previousRxRate, previousTxRate;
		long probeInterval;
		List<FileInputStream> rxFISList = new LinkedList<FileInputStream>();
		List<FileInputStream> txFISList = new LinkedList<FileInputStream>();

		Timer timer = new Timer();

		public NetworkMonitor() {
			this.probeInterval = 100;
		}

		public void stop() {
			timer.cancel();
		}

		public void start() {
			timer.scheduleAtFixedRate(new TimerTask() {

				@Override
				public void run() {
					//probe();
				}
			}, 0L, probeInterval);

			timer.scheduleAtFixedRate(new TimerTask() {

				@Override
				public void run() {
					tokenControl();
				}
			}, 0L, tokenInterval);
		}

		public void init() {
			devices = getDeviceList();
			for (String device : devices) {
				File rxFile = new File("/sys/class/net/" + device + "/statistics/rx_bytes");
				File txFile = new File("/sys/class/net/" + device + "/statistics/tx_bytes");
				try {
					this.rxFISList.add(new FileInputStream(rxFile));
					this.txFISList.add(new FileInputStream(txFile));
				} catch (Exception e) {
					LOG.error("@@ Monitor: cannot create network bandwidth reader", e);
				}
			}
		}

		public synchronized void tokenControl() {
			LOG.error("@@ Token: unused token=" + tokenQueue.size());
			tokenQueue.clear();
			for (int i = 0; i < tokenNumber; i++) {
				Token token = new Token(1, tokenSize);
				tokenQueue.add(token);
			}
		}

		public List<String> getDeviceList() {
			List<String> devices = new LinkedList<String>();
			File f = new File("/sys/class/net");
			for (String s : f.list()) {
				if (s.contains("eth")) {
					devices.add(s);
				}
			}
			return devices;
		}

		private double readDeviceValue(FileInputStream fis) throws Exception {
			fis.getChannel().position(0);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
			double value = Double.parseDouble(reader.readLine());
			return value;
		}

		public void probe() {
			LOG.error("@@ IMS: probing network status");
			double rxBytes = 0;
			double txBytes = 0;

			for (FileInputStream fis : this.rxFISList) {
				try {
					rxBytes += readDeviceValue(fis);
				} catch (Exception e) {
					LOG.error("@@ Monitor: cannot read rxBytes", e);
				}
			}

			for (FileInputStream fis : this.txFISList) {
				try {
					txBytes += readDeviceValue(fis);
				} catch (Exception e) {
					LOG.error("@@ Monitor: cannot read txBytes", e);
				}
			}
			this.previousRxBytes = this.rxBytes;
			this.previousTxBytes = this.txBytes;
			this.previousRxRate = this.rxRate;
			this.previousTxRate = this.txRate;
			this.rxBytes = rxBytes;
			this.txBytes = txBytes;
			this.rxRate = (this.rxBytes - this.previousRxBytes) / this.probeInterval;
			this.txRate = (this.txBytes - this.previousTxBytes) / this.probeInterval;
			LOG.error("@@ IMS: rxBytes=" + rxBytes + ", txBytes=" + txBytes);
			LOG.error("@@ IMS: rxRate=" + rxRate + ", txRate=" + txRate);
		}

		public double getTXBytes() {
			return txBytes;
		}

		public double getRXBytes() {
			return rxBytes;
		}

		public double getTXRate() {
			return txRate;
		}

		public double getRXRate() {
			return rxRate;
		}

	}

}
