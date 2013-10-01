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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.PrefetchInfo;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.util.PrefetchUtils;

/**
 * The class which provides functionality of checking the health of the node and
 * reporting back to the service for which the health checker has been asked to
 * report.
 */
public class InMemoryService extends CompositeService {

	private static final Log LOG = LogFactory.getLog(InMemoryService.class);
	private Context context;
	private File prefetchDir;

	public InMemoryService(Context context) {
		super(InMemoryService.class.getName());
		this.context = context;
	}

	@Override
	public void init(Configuration conf) {
		super.init(conf);
		LOG.error("@@ IMS: initialize");
		prefetchDir = new File("/dev/shm/hadoop");
		prefetchDir.mkdirs();
	}

	@Override
	public synchronized void stop() {
		super.stop();
		/**
		 * for (File file : prefetchDir.listFiles()) { file.delete(); }
		 * prefetchDir.delete();
		 */
	}

	public List<PrefetchInfo> getSplits() {
		LOG.error("@@ IMM: return prefetched splits");
		return completeList;
	}

	private List<PrefetchInfo> prefetchList = new LinkedList<PrefetchInfo>();
	private List<PrefetchInfo> completeList = new LinkedList<PrefetchInfo>();

	public void addPrefetchRequest(String splits) {
		LOG.error("@@ IMS: add prefetch request");
		if (splits == null) {
			return;
		}
		List<org.apache.hadoop.yarn.api.records.PrefetchInfo> newPrefetchList = PrefetchUtils.decode(splits);
		for (org.apache.hadoop.yarn.api.records.PrefetchInfo pi : newPrefetchList) {
			pi.node = context.getNodeId().getHost();
			pi.progress = "0";
			prefetchList.add(pi);
			PrefetchWorker pw = new PrefetchWorker(pi, this);
			prefetchService.execute(pw);
		}

	}

	public void reportPrefetchProgress(PrefetchInfo pi) {
		LOG.error("@@ IMS: report prefetching split -> " + pi);
		pi.progress = String.valueOf(pi.fileLength);
		prefetchList.remove(pi);
		completeList.add(pi);
	}

	static class PrefetchWorker implements Runnable {

		private PrefetchInfo split;
		private InMemoryService ims;

		static int sizeOfByteBuffer = 8192;
		static int sizeOfBuffer = 1024;

		public PrefetchWorker(PrefetchInfo split, InMemoryService ims) {
			this.split = split;
			this.ims = ims;
		}

		private String convertFilePath(String filePath) {
			return filePath.replace("file:", "");
		}

		@Override
		public void run() {
			try {
				LOG.error("@@ IMS: start to prefetch file: " + split);
				String filePath = convertFilePath(split.file);
				File inputFile = new File(filePath);
				LOG.error("@@ IMS: read file path=" + inputFile.getPath());
				FileInputStream fis = new FileInputStream(inputFile);
				FileChannel fc = fis.getChannel();
				fc.position(Long.parseLong(split.fileOffset));

				File prefetchOutputFile = new File(this.ims.prefetchDir, inputFile.getName() + "." + split.fileLength + "." + split.fileOffset);
				LOG.error("@@ IMS: write file path=" + prefetchOutputFile.getPath());
				FileOutputStream fos = new FileOutputStream(prefetchOutputFile);

				ByteBuffer bb = ByteBuffer.allocateDirect(sizeOfByteBuffer);
				byte[] barray = new byte[sizeOfBuffer];

				int nRead, nGet;
				int prefetchCount = 0;
				int targetCount = Integer.parseInt(split.fileLength);

				long timeStart = System.nanoTime();
				while ((nRead = fc.read(bb)) != -1 && prefetchCount < targetCount) {
					if (nRead == 0)
						continue;
					bb.position(0);
					bb.limit(nRead);
					while (bb.hasRemaining()) {
						nGet = Math.min(bb.remaining(), sizeOfBuffer);
						bb.get(barray, 0, nGet);
						fos.write(barray, 0, nGet);
					}
					bb.clear();

					prefetchCount += nRead;
				}
				long timeEnd = System.nanoTime();

				LOG.error("@@ IMS: prefetching time=" + (timeEnd - timeStart));

				fos.close();
				fc.close();
				fis.close();
				ims.reportPrefetchProgress(split);
				LOG.error("@@ IMS: finish to prefetch file: " + split);
			} catch (Exception ex) {
				LOG.fatal("@@ IMS: read nfs file failed ->" + ex);
			}
		}
	}

	ExecutorService prefetchService = Executors.newFixedThreadPool(8);

}
