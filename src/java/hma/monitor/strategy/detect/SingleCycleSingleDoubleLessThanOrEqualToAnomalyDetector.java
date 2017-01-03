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
public class SingleCycleSingleDoubleLessThanOrEqualToAnomalyDetector extends
		SingleCycleSingleDoubleAnomalyDetector {
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param threshold
	 */
	public SingleCycleSingleDoubleLessThanOrEqualToAnomalyDetector(String monitorItemName,
			double threshold) {
		super(monitorItemName, threshold);
	}
	
	/**
	 * @param monitorItemName
	 * @param threshold
	 */
	public SingleCycleSingleDoubleLessThanOrEqualToAnomalyDetector(String monitorItemName,
			Double threshold) {
		this(monitorItemName, threshold.doubleValue());
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.SingleCycleAnomalyDetector#visitMonitorStrategyData(hma.monitor.strategy.MonitorStrategyData)
	 */
	@Override
	protected AnomalyDetectionResult visitMonitorStrategyData(
			MonitorStrategyData<Double> strategyData) {
		
		double data = strategyData.getData();
		long timestamp = strategyData.getTimestamp();
		
		int alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		
		String alarmInfo = null;
		String dectionInfo = 
			"[" + dateFormat.format(new Date(timestamp)) + "] " +
			getMonitorItemName() + " = " + data;
		
		boolean anomalous = (data <= getThreshold());
		if (anomalous) {
			alarmLevel = MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			alarmInfo = 
				getMonitorItemName() + " <= " + getThreshold() + "\n" +
				"[" + dateFormat.format(new Date(timestamp)) + "] " +
				getMonitorItemName() + " = " + data;
		}
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, dectionInfo);
	}
	
}
