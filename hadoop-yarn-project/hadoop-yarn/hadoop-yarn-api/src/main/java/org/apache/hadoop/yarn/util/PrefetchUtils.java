package org.apache.hadoop.yarn.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.PrefetchInfo;

import com.google.protobuf.ByteString;

public class PrefetchUtils {

	private static final Log LOG = LogFactory.getLog(PrefetchUtils.class);

	/**
	 * format: *.job.split, offset, file, file offset, split length
	 * 
	 * @param splits
	 * @return
	 */
	public static List<PrefetchInfo> decode(String splits) {
		List<PrefetchInfo> prefetchList = new LinkedList<PrefetchInfo>();
		if (splits != null && splits.contains("@")) {
			for (String record : splits.split("@")) {
				String[] recordInfo = record.split(",");
				PrefetchInfo prefetchInfo = new PrefetchInfo();
				prefetchInfo.meta = recordInfo[0];
				prefetchInfo.metaOffset = Integer.parseInt(recordInfo[1]);
				prefetchInfo.file = recordInfo[2];
				prefetchInfo.fileOffset = Integer.parseInt(recordInfo[3]);
				prefetchInfo.fileLength = Integer.parseInt(recordInfo[4]);
				prefetchInfo.nodeId = recordInfo[5];
				prefetchInfo.progress = Integer.parseInt(recordInfo[6]);
				prefetchList.add(prefetchInfo);
			}
		}
		return prefetchList;
	}

	public static String encode(List<PrefetchInfo> prefetchList) {
		StringBuilder builder = new StringBuilder();
		for (PrefetchInfo pi : prefetchList) {
			builder.append(pi.meta);
			builder.append(",");
			builder.append(pi.metaOffset);
			builder.append(",");
			builder.append(pi.file);
			builder.append(",");
			builder.append(pi.fileOffset);
			builder.append(",");
			builder.append(pi.fileLength);
			builder.append(",");
			builder.append(pi.nodeId);
			builder.append(",");
			builder.append(pi.progress);
			builder.append("@");
		}
		return builder.toString();
	}

	public static ByteString serializedEncode(List<PrefetchInfo> prefetchList) {
		try {
			// LOG.error("@@ encode: " + prefetchList);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);
			os.writeInt(prefetchList.size());
			// LOG.error("@@ encode: list size=" + prefetchList.size());
			for (PrefetchInfo pi : prefetchList) {
				// LOG.error("@@ encode: object=" + pi);
				os.writeObject(pi);
			}
			os.flush();
			return ByteString.copyFrom(bos.toByteArray());
		} catch (Exception ex) {
			LOG.error("@@ encode: cannot serialize object -> " + ex);
		}
		return null;
	}

	public static List<PrefetchInfo> serializedDecode(ByteString bs) {
		List<PrefetchInfo> prefetchList = new LinkedList<PrefetchInfo>();
		try {
			// LOG.error("@@ decode: " + bs.size());
			ByteArrayInputStream bis = new ByteArrayInputStream(bs.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bis);
			int num = ois.readInt();
			// LOG.error("@@ decode: list size=" + num);
			for (int i = 0; i < num; i++) {
				PrefetchInfo pi = (PrefetchInfo) ois.readObject();
				prefetchList.add(pi);
				// LOG.error("@@ decode: object=" + pi);
			}
		} catch (Exception ex) {
			LOG.error("@@ decode: cannot deserialize object -> " + ex);
		}
		return prefetchList;
	}

}
