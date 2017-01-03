/**
 * 
 */
package hma.monitor.strategy.detect;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

/**
 * @author guoyezhi
 *
 */
public class SingleCycleSingleStringMatchRegExpAnomalyDetector extends
		SingleCycleSingleStringAnomalyDetector {
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param pattern
	 */
	public SingleCycleSingleStringMatchRegExpAnomalyDetector(String monitorItemName,
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
		
		Pattern p = Pattern.compile(this.getPattern());
		boolean anomalous = p.matcher(strategyData.getData()).find();
		if (anomalous) {
			alarmLevel = MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			alarmInfo = 
				getMonitorItemName() + " MATCHES '" + getPattern() + "'\n" + 
				"[" + dateFormat.format(new Date(timestamp)) + "] " +
				getMonitorItemName() + " :\n" + strategyData.getData();
		}
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, dectionInfo);
	}
	
}
