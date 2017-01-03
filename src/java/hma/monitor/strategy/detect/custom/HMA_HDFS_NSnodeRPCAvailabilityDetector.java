/**
 * 
 */
package hma.monitor.strategy.detect.custom;

import hma.monitor.MonitorManager;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.detect.AnomalyDetectionResult;
import hma.monitor.strategy.detect.SingleCycleSingleStringAnomalyDetector;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author guoyezhi
 *
 */
public class HMA_HDFS_NSnodeRPCAvailabilityDetector extends
		SingleCycleSingleStringAnomalyDetector {
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	/**
	 * @param monitorItemName
	 * @param pattern
	 */
	public HMA_HDFS_NSnodeRPCAvailabilityDetector(String monitorItemName,
			String pattern) {
		super(monitorItemName, pattern);
	}
	
	
	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.SingleCycleSingleTypeAnomalyDetector#visitMonitorStrategyData(hma.monitor.strategy.MonitorStrategyData)
	 */
	@Override
	protected AnomalyDetectionResult visitMonitorStrategyData(
			MonitorStrategyData<String> strategyData) {
		
		final String itemName = "HDFS_RPC_FileSystem_ListRootPathEntries";
		if (!getMonitorItemName().equals(itemName)) {
			throw new RuntimeException("Configuration Error");
		}
		
		if (strategyData == null || 
				(strategyData.getData() == null)) {
			return null;
		}
		String statuses = (String) strategyData.getData();
		long timestamp = strategyData.getTimestamp();
		
		boolean anomalous = false;
		int alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		String alarmInfo = null;
		String dectionInfo = 
			"[" + dateFormat.format(new Date(timestamp)) + "] " + 
			getMonitorItemName() + " :\n" + statuses;
		
		long threshold = 
			MonitorManager.getGlobalConf().getLong(
					"namenode.rpc.timeout.alarm.threshold", 300000);
		long currentTime = System.currentTimeMillis();
		anomalous = ((currentTime - timestamp) > threshold);
		if (anomalous) {
			alarmLevel = 
				MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
			alarmInfo = 
				"HDFS服务不可用" + ((currentTime - timestamp) / 1000) + "秒！";
		} else {
			this.setPattern(
					MonitorManager.getMonitoredClusterName().toLowerCase());
			anomalous = 
				!(statuses.toLowerCase().contains(this.getPattern()));
			if (anomalous) {
				alarmLevel = 
					MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
				alarmInfo = 
					"未探测到集群标志文件，HDFS根目录可能发生了误删除！\n" + 
					getMonitorItemName() + 
					" (Collected at: " + 
					dateFormat.format(new Date(timestamp)) + 
					") :\n" + 
					statuses;
			}
		}
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, dectionInfo);
		
	}
	
}
