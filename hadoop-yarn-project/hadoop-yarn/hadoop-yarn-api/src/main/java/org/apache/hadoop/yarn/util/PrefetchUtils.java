package org.apache.hadoop.yarn.util;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.PrefetchInfo;

public class PrefetchUtils {

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
				prefetchInfo.metaOffset = recordInfo[1];
				prefetchInfo.file = recordInfo[2];
				prefetchInfo.fileOffset = recordInfo[3];
				prefetchInfo.fileLength = recordInfo[4];
				prefetchInfo.node = recordInfo[5];
				prefetchInfo.progress = recordInfo[6];
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
			builder.append(pi.node);
			builder.append(",");
			builder.append(pi.progress);
			builder.append("@");
		}
		return builder.toString();
	}

}
