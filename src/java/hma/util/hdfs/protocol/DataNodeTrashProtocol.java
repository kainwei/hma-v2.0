package hma.util.hdfs.protocol;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface DataNodeTrashProtocol extends VersionedProtocol {
    public static final long versionID = 1L;

    public long getTrashRetentionSeconds();

    public static final String DFS_TRASH_RETENTION_TIME_KEY = "dfs.trash.retention.time.second";
    public static final long DFS_TRASH_RETENTION_TIME_DEFAULT_VALUE = Long.MAX_VALUE;
    public static final String DFS_TRASH_IPC_HOSTNAME_KEY = "dfs.trash.ipc.hostname";
    public static final String DFS_TRASH_IPC_HOSTNAME_DEFAULT_VALUE = "localhost";
    public static final String DFS_TRASH_IPC_PORT_KEY = "dfs.trash.ipc.port";
    public static final int DFS_TRASH_IPC_PORT_DEFAULT_VALUE = 22570;
    public static final String DFS_TRASH_IPC_HANDLER_COUNT_KEY = "dfs.trash.ipc.handler.count";
    public static final int DFS_TRASH_IPC_HANDLER_COUNT_DEFAULT_VALUE = 50;
}
