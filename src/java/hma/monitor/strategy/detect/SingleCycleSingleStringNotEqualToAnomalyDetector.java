/**
 * 
 */
package hma.monitor.strategy.detect;

import java.sql.Date;
import java.text.SimpleDateFormat;

import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

/**
 * @author guoyezhi
 *
 */
public class SingleCycleSingleStringNotEqualToAnomalyDetector extends
		SingleCycleSingleStringAnomalyDetector {
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param pattern
	 */
	public SingleCycleSingleStringNotEqualToAnomalyDetector(String monitorItemName,
			String pattern) {
		super(monitorItemName, pattern);
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.SingleCycleAnomalyDetector#visitMonitorStrategyData(hma.monitor.strategy.MonitorStrategyData)
	 */
	@Override
	protected AnomalyDetectionResult visitMonitorStrategyData(
			MonitorStrategyData<String> strategyData) {
		
		long timestamp = strategyData.getTimestamp();
		
		int alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		
		String alarmInfo = null;
		String dectionInfo = 
			"[" + dateFormat.format(new Date(timestamp)) + "] " +
			getMonitorItemName() + " :\n" + strategyData.getData();
		
		boolean anomalous =
			!(strategyData.getData().equals(this.getPattern()));
		if (anomalous) {
			alarmLevel = MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			alarmInfo = 
				getMonitorItemName() + " NOT EQUAL TO '" + getPattern() + "'\n" + 
				"[" + dateFormat.format(new Date(timestamp)) + "] " +
				getMonitorItemName() + " :\n" + strategyData.getData();
		}
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, dectionInfo);
	}
	
}
