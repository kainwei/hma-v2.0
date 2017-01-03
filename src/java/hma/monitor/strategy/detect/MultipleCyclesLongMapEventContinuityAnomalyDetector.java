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
public class MultipleCyclesLongMapEventContinuityAnomalyDetector extends
		MultipleCyclesLongMapAnomalyDetector {
	
	private static final SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 * @param singleCycleDetector
	 */
	public MultipleCyclesLongMapEventContinuityAnomalyDetector(
			String monitorItemName, int cycles, int threshold,
			SingleCycleSingleLongAnomalyDetector singleCycleDetector) {
		super(monitorItemName, cycles, singleCycleDetector);
		// nothing needed to do with @param threshold
	}
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 * @param threshold
	 * @param singleCycleDetector
	 */
	public MultipleCyclesLongMapEventContinuityAnomalyDetector(
			String monitorItemName, Integer cycles, Integer threshold,
			SingleCycleSingleLongAnomalyDetector singleCycleDetector) {
		super(monitorItemName, cycles.intValue(), singleCycleDetector);
		// nothing needed to do with @param threshold
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.MultipleCyclesAnomalyDetector#visitMultipleMonitorStrategyData(java.util.NavigableMap)
	 */
	@Override
	protected AnomalyDetectionResult visitMultipleCyclesMonitorStrategyData(
			NavigableMap<Date, MonitorStrategyData<Map<String, Long>>> multipleCyclesData) {
		
		if (multipleCyclesData.size() < this.getCycles())
			return null;
		
		StringBuilder fullInfoBuilder = new StringBuilder();
		fullInfoBuilder.append(
				"[" + dateFormat.format(multipleCyclesData.lastKey()) + "] " +
				"The last cycle detection result:\n");
		Iterator<Map.Entry<String, Long>> iter2 = 
			multipleCyclesData.lastEntry().getValue().getData().entrySet().iterator();
		while (iter2.hasNext()) {
			Map.Entry<String, Long> entry = iter2.next();
			String target = entry.getKey();
			Long value = entry.getValue();
			fullInfoBuilder.append("\t" +  target + " -> " + value + "\n");
		}
		String fullResultInfo = fullInfoBuilder.toString();
		
		
		Date firstKey = multipleCyclesData.firstKey();
		int i = this.getCycles();
		Date d = firstKey;
		Set<String> set = 
			multipleCyclesData.lastEntry().getValue().getData().keySet();
		
		Map<String, Long> toDetect = null;
		Map<String, Long> detected = null;
		
		SingleCycleSingleLongAnomalyDetector singleCycleDetector = 
			this.getSingleDetector();
		
		while (d != null && i > 0 && (set.size() != 0)) {
			
			toDetect = multipleCyclesData.get(d).getData();
			detected = new HashMap<String, Long>();
			
			Iterator<String> iter = set.iterator();
			
			while (iter.hasNext()) {
				String key = iter.next();
				Long value = toDetect.get(key);
				if (value == null) continue;
				MonitorStrategyData<Long> wrapper =
					new MonitorStrategyData<Long>(null, value, i);
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
		String keyResultInfo = null;
		StringBuilder keyInfoBuilder = new StringBuilder();
		
		if (i == 0 && (set.size() > 0)) {
			alarmLevel =
				MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			
			keyInfoBuilder.append(
					"Total Anomalous Target Num: " + set.size() + "\n");
			keyInfoBuilder.append("Anomalous Target Status of last cycle:\n");
			Iterator<Map.Entry<String, Long>> iter = 
				detected.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, Long> entry = iter.next();
				String target = entry.getKey();
				Long value = entry.getValue();
				keyInfoBuilder.append("\t" +  target + " -> " + value + "\n");
			}
			keyResultInfo = keyInfoBuilder.toString();

			return new AnomalyDetectionResult(
					true, alarmLevel, keyResultInfo, fullResultInfo);
		}
		
		return new AnomalyDetectionResult(
				false, alarmLevel, keyResultInfo, fullResultInfo);
	}
	
}
