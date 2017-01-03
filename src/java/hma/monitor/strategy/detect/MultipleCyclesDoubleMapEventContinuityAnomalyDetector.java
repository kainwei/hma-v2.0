/**
 * 
 */
package hma.monitor.strategy.detect;

import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * @author guoyezhi
 *
 */
public class MultipleCyclesDoubleMapEventContinuityAnomalyDetector extends
		MultipleCyclesDoubleMapAnomalyDetector {
	
	private static final SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 * @param threshold
	 * @param singleCycleDetector
	 */
	public MultipleCyclesDoubleMapEventContinuityAnomalyDetector(
			String monitorItemName, int cycles, int threshold,
			SingleCycleSingleDoubleAnomalyDetector singleCycleDetector) {
		super(monitorItemName, cycles, singleCycleDetector);
		// nothing needed to do with @param threshold
	}
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 * @param threshold
	 * @param singleCycleDetector
	 */
	public MultipleCyclesDoubleMapEventContinuityAnomalyDetector(
			String monitorItemName, Integer cycles, Integer threshold,
			SingleCycleSingleDoubleAnomalyDetector singleCycleDetector) {
		super(monitorItemName, cycles.intValue(), singleCycleDetector);
		// nothing needed to do with @param threshold
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.MultipleCyclesMapTypeAnomalyDetector#visitMultipleCyclesMonitorStrategyData(java.util.NavigableMap)
	 */
	@Override
	protected AnomalyDetectionResult visitMultipleCyclesMonitorStrategyData(
			NavigableMap<Date, MonitorStrategyData<Map<String, Double>>> multipleCyclesData) {
		
		if (multipleCyclesData.size() < this.getCycles()) {
			return null;
		}
		
		StringBuilder detectionInfoBuilder = new StringBuilder();
		detectionInfoBuilder.append(
				"[" + dateFormat.format(multipleCyclesData.lastKey()) + "] " +
				"The last cycle detection result:\n");
		Iterator<Map.Entry<String, Double>> iter2 = 
			multipleCyclesData.lastEntry().getValue().getData().entrySet().iterator();
		while (iter2.hasNext()) {
			Map.Entry<String, Double> entry = iter2.next();
			String target = entry.getKey();
			Double value = entry.getValue();
			detectionInfoBuilder.append("\t" +  target + " -> " + value + "\n");
		}
		String detectionInfo = detectionInfoBuilder.toString();
		
		
		Date firstKey = multipleCyclesData.firstKey();
		int i = this.getCycles();
		Date d = firstKey;
		Set<String> set = 
			multipleCyclesData.firstEntry().getValue().getData().keySet();
		
		Map<String, Double> toDetect = null;
		Map<String, Double> detected = null;
		
		SingleCycleSingleDoubleAnomalyDetector singleCycleDetector = 
			this.getSingleDetector();
		
		while (d != null && i > 0 && (set.size() != 0)) {
			
			toDetect = multipleCyclesData.get(d).getData();
			detected = new HashMap<String, Double>();
			
			Iterator<String> iter = set.iterator();
			
			while (iter.hasNext()) {
				String key = iter.next();
				Double value = toDetect.get(key);
				if (value == null) continue;
				MonitorStrategyData<Double> wrapper =
					new MonitorStrategyData<Double>(null, value, i);
				AnomalyDetectionResult res = 
					singleCycleDetector.visitMonitorStrategyData(wrapper);
				if (res.isAnomalous() == true) {
					detected.put(key, value);
				}
			}
			
			d = multipleCyclesData.higherKey(d);
			i--;
			set = detected.keySet();
		}
		
		int alarmLevel =
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		String alarmInfo = null;
		StringBuilder alarmInfoBuilder = new StringBuilder();
		
		if (i == 0 && (set.size() > 0)) {
			alarmLevel =
				MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			
			alarmInfoBuilder.append(
					"Total Anomalous Target Num: " + set.size() + "\n");
			alarmInfoBuilder.append("Anomalous Target Status of last cycle:\n");
			Iterator<Map.Entry<String, Double>> iter = 
				detected.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, Double> entry = iter.next();
				String target = entry.getKey();
				Double value = entry.getValue();
				alarmInfoBuilder.append("\t" +  target + " -> " + value + "\n");
			}
			alarmInfo = alarmInfoBuilder.toString();

			return new AnomalyDetectionResult(
					true, alarmLevel, alarmInfo, detectionInfo);
		}
		
		return new AnomalyDetectionResult(
				false, alarmLevel, alarmInfo, detectionInfo);
	}
	
}
