package hma.web;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: synckey
 * Date: 13-9-22
 * Time: 下午2:17
 * To change this template use File | Settings | File Templates.
 */
public class HmaInspection extends HttpServlet {
    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        Map<String, String> inspectEntry = new LinkedHashMap<String, String>();
        inspectEntry.put("HDFS_TotalStorageURate_MON", "HdfsUse");
        inspectEntry.put("HDFS_Http4Inode_Collector", "Inodes");
        inspectEntry.put("HDFS_Http4Blocks_Collector", "Blocks");
        inspectEntry.put("HDFS_MissingBlocks_MON", "MissingBlk");
        inspectEntry.put("HDFS_RecentlyRsyncedMetadataFileStatus_MON", "RsyncStat");
        inspectEntry.put("HDFS_LiveDatanodeXceiverUsage_MON", "Xceiver");
        inspectEntry.put("HDFS_LiveDatanodeStorageUsageRate_MON", "DnUse");
        inspectEntry.put("HDFS_LiveDatanodeNonDFSStorageUsage_MON", "NonDfsUse");
        inspectEntry.put("HDFS_LiveDatanodeHeartBeatInterval_MON", "DnHeart");
        inspectEntry.put("HDFS_MultiDatanodeVolumeCorruption_MON", "VolCrpt");
        inspectEntry.put("Datanode_LiveToDeadRepeatedly_Mon", "ReDeadLive");
        inspectEntry.put("Namenode_CheckPointStatus_Mon", "CheckPoint Status");
    }

}
