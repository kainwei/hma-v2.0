/**
 * 
 */
package hma.monitor.collection.custom;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemWorker;
import hma.monitor.collection.metric.CollectionMetricPool;
import hma.monitor.collection.metric.CollectionMetricRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * @author guoyezhi
 *
 */
public class HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus_MonitorItemWorker 
extends MonitorItemWorker<String, List<FileStatus>> {
	
	public static final String className = 
		HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus_MonitorItemWorker.class.getSimpleName();
	
	public static final Log LOG = 
	    LogFactory.getLog(HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus_MonitorItemWorker.class);
	
	public static final String MONITOR_ITEM_NAME =
		"HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus";
	
	private static SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private Runtime rt = null;
	
	
	public HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus_MonitorItemWorker() {
		rt = Runtime.getRuntime();
	}
	
	
	@Override
	public MonitorItemData<String> collect() {
		
		Configuration conf = MonitorManager.getGlobalConf();
		//String server = conf.get("rsynced.metadata.backup.machine");
		//String server = conf.getStrings("rsynced.metadata.backup.machine");
		//String[] server = conf.get("rsynced.metadata.backup.machine").split(",");
		String server = null;
		String[] machine_dir = conf.get("rsynced.metadata.backup.machine.and.dir").split(",");

		//if (server == null) {			throw new RuntimeException(); }

//		String dir = conf.get(	"recently.rsynced.metadata.directory","/home/work/hadoop-rsync/meta/name/current");
		
		StringBuffer buffer = new StringBuffer();
		StringBuilder sb = new StringBuilder();
		long acquisitionTimestamp = System.currentTimeMillis();
		long begTimestamp = acquisitionTimestamp;
		long endTimestamp = 0;
		
		InputStream in = null;
		//InputStream err = null;
		
		BufferedReader reader = null;
    for(int i = 0; i < machine_dir.length; i++) {
		String[] one_machine_dir = machine_dir[i].split(":");
		server = one_machine_dir[0];
	int count =0;
	for(int j = 1; j < one_machine_dir.length; j++) {
			String dir = one_machine_dir[j];
		try {
			
			sb.append("\n\t[" + dateFormat.format(new Date(begTimestamp)) + " - " 
					+ className + "] Getting " + MONITOR_ITEM_NAME + " ...");
			in = rt.exec("safewrapper 12 /usr/bin/ssh "
					+ "-o StrictHostKeyChecking=no "
					+ "-o PasswordAuthentication=no "
					+ "-o ConnectTimeout=10 "
					+ "rd@"
					+ server + " "
					+ "stat -t " + dir + "/*").getInputStream();
			reader = new BufferedReader(new InputStreamReader(in));
			String str = null;
			count = 0;
			while ((str = reader.readLine()) != null) {
				count++;
				sb.append("\n\t[" + dateFormat.format(new Date(begTimestamp)) + " - " 
						+ className + "] -> server: "+ server + " " + str + "\n");
				buffer.append(str + " " + server + ":" + dir + "\n");
				//buffer.append(str +  "\n");
			}
			if(count ==0){
				LOG.fatal(MONITOR_ITEM_NAME+" this "+server+" collect error !+++++error+++++error+++++");
				return null;
			}
			endTimestamp = System.currentTimeMillis();
			sb.append("\n\t[" + dateFormat.format(new Date(endTimestamp)) + " - " 
					+ className + "] Have got " + MONITOR_ITEM_NAME);
			
		} catch (Throwable e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (reader != null) reader.close();
				if (in != null) in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
    }
		
		CollectionMetricPool.offerRecord(
				new CollectionMetricRecord(
						MonitorManager.getMonitoredClusterName(),
						MONITOR_ITEM_NAME, 
						begTimestamp, 
						endTimestamp - begTimestamp));
		
		LOG.info(sb.toString());
		LOG.info(buffer.toString());
		
		if (buffer.length() > 0) {
			return new MonitorItemData<String>(
					MONITOR_ITEM_NAME, buffer.toString(), acquisitionTimestamp);
		}
		
		return null;
	}
	
	@Override
	public MonitorItemData<List<FileStatus>> extract(MonitorItemData<String> rawData) {
		
		Configuration conf = MonitorManager.getGlobalConf();
		String dir = conf.get(
				"recently.rsynced.metadata.directory",
				"/home/work/hadoop-rsync/meta/name/current");
		
		String[] fsArray = rawData.getData().split("\n");
		List<FileStatus> fsList = new ArrayList<FileStatus>();
		
		for (int i = 0; i < fsArray.length; i++) {
			
			/*
			 * Size:4935289049 Access:1281627657 Modify:1290958135
			 *               file name                          |   size    | blocks |    |uid|gid|dev|  inode | | | |  access  |  modify  |  change  |    
			 * /home/work/hadoop-rsync/meta/last_minutes/fsimage 13348683493 26097152 81b4 503 505 803 35438610 1 0 0 1292418968 1292803033 1292803033 4096
			 */
			String[] statusArray = fsArray[i].split(" ");
			
			if (statusArray.length == 16) {
//			if (statusArray.length == 15 && statusArray[0].startsWith(dir)) {
				String fname = statusArray[0];
				long size = Long.parseLong(statusArray[1]);
				long modificationTime = Long.parseLong(statusArray[12]) * 1000;
				String owner = statusArray[15];
				
				//fsList.add(new FileStatus(size, false, 0, 0, modificationTime, new Path(fname)));
				fsList.add(new FileStatus(size, false, 0, 0, modificationTime, 0, null, owner, null, new Path(fname)));
			} else {
				LOG.warn("Invalid RawData Format: <" 
						+ fsArray[i]
						+ "> : Total " + statusArray.length + " fields");
			}
			
		}
		
		if (fsList.size() > 0) {
			return new MonitorItemData<List<FileStatus>>(rawData.getName(), 
					fsList, rawData.getTimestamp());
		}
		
		LOG.error("Extract Data Failed !");
		
		return null;
	}
	
	
	/**
	 * for unit testing
	 * 
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		
		HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus_MonitorItemWorker worker =
			new HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus_MonitorItemWorker();
		
		FileStatus status = 
			(FileStatus) worker.extract(worker.collect()).getData();
		System.out.println("Fsimage Size: " + status.getLen());
		System.out.println("Fsimage modified at: "
				+ new Date(status.getModificationTime()));
		
	}
	
}

