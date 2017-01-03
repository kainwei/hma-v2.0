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
public class SingleCycleSingleStringNotEmptyAnomalyDetector extends
		SingleCycleSingleStringAnomalyDetector {
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param pattern
	 */
	public SingleCycleSingleStringNotEmptyAnomalyDetector(
			String monitorItemName, String pattern) {
		super(monitorItemName, pattern);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.SingleCycleSingleTypeAnomalyDetector#visitMonitorStrategyData(hma.monitor.strategy.MonitorStrategyData)
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
		
		boolean anomalous = !(strategyData.getData().isEmpty());
		if (anomalous) {
			alarmLevel = MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			alarmInfo = 
				this.getMonitorItemName() + " IS EMPTY"+ "\n" + 
				"[" + dateFormat.format(new Date(timestamp)) + "] " +
				getMonitorItemName() + " :\n" + strategyData.getData();
		}
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, dectionInfo);
	}
	
}
