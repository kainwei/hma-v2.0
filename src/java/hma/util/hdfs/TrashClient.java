package hma.util.hdfs;

import hma.util.hdfs.protocol.DataNodeTrashProtocol;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created with IntelliJ IDEA.
 * User: wangjian
 * Date: 2013-11-07
 * Time: 22:44:00
 */

public class TrashClient {
    public static final Log LOG = LogFactory.getLog(TrashClient.class);
    public DataNodeTrashProtocol trashServer = null;
    private org.apache.hadoop.conf.Configuration conf = null;

    public TrashClient(Configuration conf) throws IOException {
        //super(conf);
        this.conf = conf;
        String hostname = this.conf.get(DataNodeTrashProtocol.DFS_TRASH_IPC_HOSTNAME_KEY,
                null);
        int port = this.conf.getInt(DataNodeTrashProtocol.DFS_TRASH_IPC_PORT_KEY,
                0);
        if (null == hostname) {
            LOG.error(DataNodeTrashProtocol.DFS_TRASH_IPC_HOSTNAME_KEY + " is not given in conf ");
        }
        if (0 == port) {
            LOG.error(DataNodeTrashProtocol.DFS_TRASH_IPC_PORT_KEY + " is not given in conf");
        }
        InetSocketAddress trashServerAddr = new InetSocketAddress(hostname, port);
        try {
            trashServer = (DataNodeTrashProtocol) RPC.getProxy(DataNodeTrashProtocol.class,
                    DataNodeTrashProtocol.versionID, trashServerAddr, conf);
        } catch (Throwable t) {
            trashServer = null;
        }
    }

    public long getTrashRetentionSecondsInternal() {
        long retentionSeconds = Long.MIN_VALUE;
        try {
            retentionSeconds = trashServer.getTrashRetentionSeconds();
        } catch (Throwable t) {
            LOG.error("failed to get retentionSeconds from trash server");
        }
        return retentionSeconds;
    }

    public void close() {
        RPC.stopProxy(trashServer);
    }

    public static long getTrashRetentionSeconds(Configuration conf) {
        TrashClient trashClient = null;
        try {
            trashClient = new TrashClient(conf);
            return trashClient.getTrashRetentionSecondsInternal();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Failed to connect to trash Server");
            return Long.MIN_VALUE;
        }

    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String trashServer = args[0];
        String port = args[1];
        System.out.println(trashServer);
        System.out.println(port);
        conf.set(DataNodeTrashProtocol.DFS_TRASH_IPC_HOSTNAME_KEY, trashServer);
        conf.set(DataNodeTrashProtocol.DFS_TRASH_IPC_PORT_KEY, port);

        System.out.println(TrashClient.getTrashRetentionSeconds(conf));

    }


}
