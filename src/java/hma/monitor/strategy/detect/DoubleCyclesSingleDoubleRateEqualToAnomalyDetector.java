/**
 * 
 */
package hma.monitor.strategy.detect;

import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NavigableMap;

/**
 * @author guoyezhi
 *
 */
public class DoubleCyclesSingleDoubleRateEqualToAnomalyDetector extends
		DoubleCyclesSingleDoubleAnomalyDetector {
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param threshold
	 */
	public DoubleCyclesSingleDoubleRateEqualToAnomalyDetector(
			String monitorItemName, double threshold) {
		super(monitorItemName, threshold);
	}
	
	/**
	 * @param monitorItemName
	 * @param threshold
	 */
	public DoubleCyclesSingleDoubleRateEqualToAnomalyDetector(
			String monitorItemName, Double threshold) {
		this(monitorItemName, threshold.doubleValue());
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.DoubleCyclesSingleTypeAnomalyDetector#visitDoubleCyclesMonitorStrategyData(java.util.NavigableMap)
	 */
	@Override
	protected AnomalyDetectionResult visitDoubleCyclesMonitorStrategyData(
			NavigableMap<Date, MonitorStrategyData<Double>> doubleCyclesData) {
		
		if (doubleCyclesData.size() < DOUBLE_CYCLES_CONSTANT) {
			return null;
		}
		
		Date date1 = doubleCyclesData.firstKey();
		Date date2 = doubleCyclesData.lastKey();
		Long time = date2.getTime() - date1.getTime();
		
		Double data1 = doubleCyclesData.get(date1).getData();
		Double data2 = doubleCyclesData.get(date2).getData();
		
		int alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		
		String alarmInfo = null;
		String detectionInfo = 
			"[" + dateFormat.format(date2) + "] " +
			getMonitorItemName() + " = " + data2 + 
			"  ->  " +
			"[" + dateFormat.format(date1) + "] " +
			getMonitorItemName() + " = " + data1;
		
		boolean anomalous = (((data2 - data1) / time) == this.getThreshold());
		if (anomalous) {
			alarmLevel = MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			alarmInfo = 
				getMonitorItemName() + " == " + getThreshold() + "\n" + detectionInfo;
		}
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, detectionInfo);
	}
	
}
