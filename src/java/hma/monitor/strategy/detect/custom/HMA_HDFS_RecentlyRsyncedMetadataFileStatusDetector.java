/**
 * 
 */
package hma.monitor.strategy.detect.custom;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.detect.AnomalyDetectionResult;
import hma.monitor.strategy.detect.SingleCycleCompoundTypeAnomalyDetector;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;

import java.sql.SQLException;
import java.text.ParseException;
import hma.util.InspectionDataInjector;

/**
 * @author guoyezhi
 *
 */
public class HMA_HDFS_RecentlyRsyncedMetadataFileStatusDetector extends
		SingleCycleCompoundTypeAnomalyDetector {
	
	private static final SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	/**
	 * @param monitorItemName
	 */
	public HMA_HDFS_RecentlyRsyncedMetadataFileStatusDetector(String monitorItemName) {
		super(monitorItemName);
	}
	
	
	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.SingleCycleCompoundTypeAnomalyDetector#visitMonitorStrategyDataCollection(hma.monitor.strategy.MonitorStrategyDataCollection)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected AnomalyDetectionResult visitMonitorStrategyDataCollection(
			MonitorStrategyDataCollection strategyDataCollection) {
		
		final String itemName = 
			"HDFS_RtExec_SecondaryNamenode_RecentlyRsyncedMetadataFileStatus";
		if (!getMonitorItemName().equals(itemName)) {
			throw new RuntimeException("Configuration Error");
		}
		
		MonitorStrategyData<?> strategyData =
			strategyDataCollection.get(itemName);
		if (strategyData == null || 
				(strategyData.getData() == null)) {
			return null;
		}
		List<FileStatus> statusList = 
			(List<FileStatus>) strategyData.getData();
		long timestamp = strategyData.getTimestamp();
		
		Configuration conf = MonitorManager.getGlobalConf();
		long newEditsFSizeThreshold =
			conf.getLong("recently.rsynced.new.edits.file.size.alarm.threshold", 
					400);
		long editsMTimeThreshold =
			conf.getLong("recently.rsynced.edits.file.mtime.alarm.threshold", 
					45);
		long fsimageMTimeThreshold = 
			conf.getLong("recently.rsynced.fsimage.file.mtime.alarm.threshold", 
					120);
		long currentTime = System.currentTimeMillis();
		
		boolean anomalous = false;
		int alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		StringBuilder alarmInfoBuilder = new StringBuilder();
		StringBuilder detectionInfoBuilder = new StringBuilder();
        StringBuilder additionalInfoBuilder = new StringBuilder();
		
		int i = 0;
		Map<String, Map<String,String>> checkPointMap = new LinkedHashMap<String,Map<String,String>>();
		for (FileStatus status : statusList) {
			
			String name = status.getPath().getName();
			long mtime = status.getModificationTime();
			String owner = status.getOwner();
			
			if (name.endsWith("edits")) {
				
				detectionInfoBuilder.append(
						"\tedits file is rsynced at " + 
						dateFormat.format(new Date(mtime)) + ".\n");
				
				if (currentTime - mtime >= editsMTimeThreshold * 60 * 1000) {
					anomalous = true;
					alarmLevel = 
						MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
					alarmInfoBuilder.append(
							(++i) + ") " + owner + " : edits file NOT UPDATED more than " + 
							editsMTimeThreshold + " mins.  ");
                    if (additionalInfoBuilder.length() > 0) {
                        additionalInfoBuilder.append(",");
                    }
                    additionalInfoBuilder.append(owner);
				}
				
			} else if (name.endsWith("edits.new")) {
				
				detectionInfoBuilder.append(
						"\tedits.new file size = " + status.getLen() + "\n");
				
				if (status.getLen() >= (newEditsFSizeThreshold * 1024 * 1024)) {
					anomalous = true;
					alarmLevel = 
						MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
					alarmInfoBuilder.append(
							(++i) + ") " + owner + " : edits.new file size >= " + 
							newEditsFSizeThreshold + "MB.  ");
                    if (additionalInfoBuilder.length() > 0) {
                        additionalInfoBuilder.append(",");
                    }
                    additionalInfoBuilder.append(owner);
				}
				
			} else if (name.endsWith("fsimage")) {
				
				detectionInfoBuilder.append(
						"\tfsimage file is rsynced at " + 
						dateFormat.format(new Date(mtime)) + ".\n");
				
				if (currentTime - mtime >= fsimageMTimeThreshold * 60 * 1000) {
					anomalous = true;
					alarmLevel = 
						MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
					alarmInfoBuilder.append(
							(++i) + ") " + owner + " : fsimage file NOT UPDATED more than " + 
							fsimageMTimeThreshold + " mins.  ");
                    if (additionalInfoBuilder.length() > 0) {
                        additionalInfoBuilder.append(",");
                    }
                    additionalInfoBuilder.append(owner);
				}
				
			}
			if (!checkPointMap.containsKey(owner)){
				Map<String,String> metaInfo = new HashMap<String,String>();
				checkPointMap.put(owner,metaInfo);	
			}
			checkPointMap.get(owner).put(name,Long.toString(status.getLen()) + ";" 
			                               + Long.toString(mtime));
			 //System.out.println(dateFormat.format(new Date()) + " kain's log about chkpoit status gen : " + "machine is :  " 
			   //                  + owner + " meta : " + name + " size&mtime: " + Long.toString(status.getLen())
			     //                + " " + Long.toString(mtime));
		}
		
	    //new HDFS_Namenode_CheckpointStatusDisplay().CheckpointStatusDisplayer(checkPointMap);
		
		String alarmInfo = 
			alarmInfoBuilder.toString().trim() + "\n" + detectionInfoBuilder.toString();
		String detectionInfo =
			"[" + dateFormat.format(new Date(timestamp)) + "] " + 
			"Recently-rsynced meta file status :\n" + 
			detectionInfoBuilder.toString();

                String clusterName = MonitorManager.getMonitoredClusterName();
                String strategyName = strategyDataCollection.getStrategyName();
                String detectionTimestamp = dateFormat.format(new Date(timestamp));
                String infoLevel = MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(alarmLevel);
                String infoContent = (alarmLevel == 
     			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED ? "Normal":"AbNormal");	
		
                try {
                        new InspectionDataInjector(clusterName,strategyName,detectionTimestamp,infoLevel,infoContent);
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                      e.printStackTrace();
                } catch (ParseException e) {
                   // TODO Auto-generated catch block
                      e.printStackTrace();
                }
                //System.out.println("kain's inspection's log : " + clusterName + " " + strategyName + " " + detectionTimestamp + " " + infoLevel + " " + infoContent);
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, detectionInfo);
		
	}
	
}
