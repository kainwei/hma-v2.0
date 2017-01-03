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

/**
 * @author guoyezhi
 *
 */
public class MultipleCyclesLongMapEventProbabilityAnomalyDetector extends
		MultipleCyclesLongMapAnomalyDetector {
	
	private int occurenceThreshold = 0;
	
	private static final SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 * @param occurenceThreshold
	 * @param singleCycleDetector
	 */
	public MultipleCyclesLongMapEventProbabilityAnomalyDetector(
			String monitorItemName, int cycles, int threshold,
			SingleCycleSingleLongAnomalyDetector singleCycleDetector) {
		super(monitorItemName, cycles, singleCycleDetector);
		this.occurenceThreshold = threshold;
	}
	
	public MultipleCyclesLongMapEventProbabilityAnomalyDetector(
			String monitorItemName, Integer cycles, Integer threshold,
			SingleCycleSingleLongAnomalyDetector singleCycleDetector) {
		this(monitorItemName, cycles.intValue(), threshold.intValue(),
				singleCycleDetector);
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
		Iterator<Map.Entry<String, Long>> it = 
			multipleCyclesData.lastEntry().getValue().getData().entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Long> entry = it.next();
			String target = entry.getKey();
			Long value = entry.getValue();
			fullInfoBuilder.append("\t" +  target + " -> " + value + "\n");
		}
		String fullResultInfo = fullInfoBuilder.toString();
		
		
		int i = this.getCycles();
		Date d = multipleCyclesData.lastKey();
		Map<String, Long> toDetect = null;
		SingleCycleSingleLongAnomalyDetector singleCycleDetector = 
			this.getSingleDetector();
		Map<String, Integer> detectionVar = new HashMap<String, Integer>();
		
		while (d != null && i > 0) {
			
			toDetect = multipleCyclesData.get(d).getData();
			
			Iterator<Map.Entry<String, Long>> iter = toDetect.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, Long> entry = iter.next();
				String key = entry.getKey();
				Long value = entry.getValue();
				if (value == null) continue;
				MonitorStrategyData<Long> wrapper =
					new MonitorStrategyData<Long>(null, value, i);
				AnomalyDetectionResult res = 
					singleCycleDetector.visitMonitorStrategyData(wrapper);
				if (res.isAnomalous() == true) {
					Integer occurences = detectionVar.get(key);
					if (occurences == null) {
						detectionVar.put(key, 1);
					} else {
						detectionVar.put(key, occurences + 1);
					}
				}
			}
			
			d = multipleCyclesData.lowerKey(d);
			i--;
		}
		
		int alarmLevel =
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		String keyResultInfo = null;
		StringBuilder keyInfoBuilder = new StringBuilder();
		Map<String, Integer> detectionResult = new HashMap<String, Integer>();
		
		if (i == 0 && (detectionVar.size() > 0)) {
			Iterator<Map.Entry<String, Integer>> iter =
				detectionVar.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, Integer> entry = iter.next();
				String key = entry.getKey();
				Integer value = entry.getValue();
				if (value >= this.occurenceThreshold) {
					detectionResult.put(key, value);
				}
			}
			if (detectionResult.size() > 0) {
				alarmLevel =
					MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
				
				keyInfoBuilder.append("Total Anomalous Target Num: " 
						+ detectionResult.size() + "\n");
				keyInfoBuilder.append("Anomalous Target Status of last cycle:\n");
				Iterator<Map.Entry<String, Integer>> iter2 = detectionResult.entrySet().iterator();
				while (iter2.hasNext()) {
					Map.Entry<String, Integer> entry = iter2.next();
					String target = entry.getKey();
					Integer value = entry.getValue();
					keyInfoBuilder.append("\t" +  target + " : " 
							+ value + " / " + this.getCycles() + "\n");
				}
				keyResultInfo = keyInfoBuilder.toString();
				
				return new AnomalyDetectionResult(
						true, alarmLevel, keyResultInfo, fullResultInfo);
			}
		}
		
		return new AnomalyDetectionResult(
				false, alarmLevel, keyResultInfo, fullResultInfo);
	}
	
}
