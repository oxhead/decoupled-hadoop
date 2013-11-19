package org.apache.hadoop.yarn.api.records;

import java.io.Serializable;

public class PrefetchInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public String meta = null;
	public Integer metaOffset = null;
	public String file = null;
	public Integer fileOffset = null;
	public Integer fileLength = null;
	public String nodeId = null;
	public Integer progress = null;
	public String taskId = null;
	public String containerId = null;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((file == null) ? 0 : file.hashCode());
		result = prime * result + ((fileLength == null) ? 0 : fileLength.hashCode());
		result = prime * result + ((fileOffset == null) ? 0 : fileOffset.hashCode());
		result = prime * result + ((meta == null) ? 0 : meta.hashCode());
		result = prime * result + ((metaOffset == null) ? 0 : metaOffset.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PrefetchInfo other = (PrefetchInfo) obj;
		if (file == null) {
			if (other.file != null)
				return false;
		} else if (!file.equals(other.file))
			return false;
		if (fileLength == null) {
			if (other.fileLength != null)
				return false;
		} else if (!fileLength.equals(other.fileLength))
			return false;
		if (fileOffset == null) {
			if (other.fileOffset != null)
				return false;
		} else if (!fileOffset.equals(other.fileOffset))
			return false;
		if (meta == null) {
			if (other.meta != null)
				return false;
		} else if (!meta.equals(other.meta))
			return false;
		if (metaOffset == null) {
			if (other.metaOffset != null)
				return false;
		} else if (!metaOffset.equals(other.metaOffset))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PrefetchInfo [meta=" + meta + ", metaOffset=" + metaOffset + ", file=" + file + ", fileOffset=" + fileOffset + ", fileLength=" + fileLength + ", nodeId=" + nodeId + ", progress="
				+ progress + ", taskId=" + taskId + "]";
	}


}
