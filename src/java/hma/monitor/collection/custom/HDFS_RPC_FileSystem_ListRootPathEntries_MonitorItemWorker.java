/**
 * 
 */
package hma.monitor.collection.custom;

import hma.monitor.MonitorManager;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemWorker;
import hma.monitor.collection.metric.CollectionMetricPool;
import hma.monitor.collection.metric.CollectionMetricRecord;
import hma.util.EasyWebGet;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author guoyezhi
 *
 */
public class HDFS_RPC_FileSystem_ListRootPathEntries_MonitorItemWorker extends
		MonitorItemWorker<String, String> {
	
	public static final String className = 
		HDFS_RPC_FileSystem_ListRootPathEntries_MonitorItemWorker.class.getSimpleName();
	
	public static final Log LOG = 
	    LogFactory.getLog(HDFS_RPC_FileSystem_ListRootPathEntries_MonitorItemWorker.class);
	
	public static final String MONITOR_ITEM_NAME = 
		"HDFS_RPC_FileSystem_ListRootPathEntries";
	
	private static SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private Configuration hadoopConf = null;
	
	
	public HDFS_RPC_FileSystem_ListRootPathEntries_MonitorItemWorker() {
		String hadoopConfDir = 
			MonitorManager.getMonitorManager().getConf().get(
					"hadoop.client.conf.dir");
		hadoopConf = new Configuration(false);
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-default.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-site.xml"));
	}
	
	
	/* (non-Javadoc)
	 * @see hma.monitor.collection.MonitorItemWorker#collect()
	 */
	@Override
	public MonitorItemData<String> collect() {
		
		FileSystem fs = null;
		StringBuilder sb = new StringBuilder();
		long begTimestamp = 0;
		long acquisitionTimestamp = 0;
		long endTimestamp = 0;
        StringBuilder keyBuilder = new StringBuilder();
        StringBuilder additionalInfoBuilder = new StringBuilder();
		
		try {
			begTimestamp = System.currentTimeMillis();
			sb.append("\n\t[" + dateFormat.format(new Date(begTimestamp)) + " - " 
					+ className + "] Getting DistributedFileSystem Proxy ...");
			fs = FileSystem.get(hadoopConf);
			sb.append("\n\t[" + dateFormat.format(new Date()) + " - "
					+ className + "] Have got DistributedFileSystem Proxy");
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	     //System.out.println("kain's log"+fs.getWorkingDirectory());	
		StringBuffer buffer = new StringBuffer();
		try {
			acquisitionTimestamp = System.currentTimeMillis();
			sb.append("\n\t[" + dateFormat.format(new Date(acquisitionTimestamp)) + " - " 
					+ className + "] Getting " + MONITOR_ITEM_NAME + " ...");
			//System.out.println("kain's log start") ;
			FileStatus[] statusArray = fs.listStatus(new Path("/"));
			//System.out.println("kain's log end fs list") ;
			endTimestamp = System.currentTimeMillis();
			sb.append("\n\t[" + dateFormat.format(new Date(endTimestamp)) + " - " 
					+ className + "] Have got " + MONITOR_ITEM_NAME);
			
			buffer.append("Found " + statusArray.length + " items:\n");
			for (FileStatus status : statusArray) {
				buffer.append(status.getPermission() + "\t"
						+ status.getReplication() + "\t"
						+ status.getOwner() + "\t"
						+ status.getGroup() + "\t"
						+ status.getLen() + "\t"
						+ new Date(status.getModificationTime()) + "\t"
						+ status.getPath().toUri().getPath() + "\n");
			}
			//System.out.println("kain's log " + buffer.toString()) ;
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			/*
			 * Notice: 
			 *     Should not close DistributedFileSystem which 
			 *     is created by invoking static method 
			 *     FileSystem.get(Configuration), otherwise the 
			 *     other threads which are using DistributedFileSystem 
			 *     will go wrong!
			 * 
			 * if (fs != null) {
			 *     try {
			 *         fs.close();
			 *     } catch (IOException e) {
			 *         e.printStackTrace();
			 *     }
			 * }
			 */
		}
		
		CollectionMetricPool.offerRecord(
				new CollectionMetricRecord(
						MonitorManager.getMonitoredClusterName(),
						MONITOR_ITEM_NAME, 
						begTimestamp, 
						endTimestamp - begTimestamp));
		
		LOG.info(sb.toString());
		
		return new MonitorItemData<String>(
				MONITOR_ITEM_NAME, buffer.toString(), acquisitionTimestamp);
	}
	
	/* (non-Javadoc)
	 * @see hma.monitor.collection.MonitorItemWorker#extract(hma.monitor.collection.MonitorItemData)
	 */
	@Override
	public MonitorItemData<String> extract(MonitorItemData<String> rawData) {
		return rawData;
	}
	
	
	/**
	 * for unit testing
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		HDFS_RPC_FileSystem_ListRootPathEntries_MonitorItemWorker worker = 
			new HDFS_RPC_FileSystem_ListRootPathEntries_MonitorItemWorker();
		MonitorItemData<String> data = worker.extract(worker.collect());
		System.out.println(data.getName() + ": <"
				+ new Date(data.getTimestamp()) + ">");
		System.out.println(data.getData());
		
	}
}

