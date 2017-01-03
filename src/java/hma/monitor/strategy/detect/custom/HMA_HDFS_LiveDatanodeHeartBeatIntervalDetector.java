/**
 * 
 */
package hma.monitor.strategy.detect.custom;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.detect.AnomalyDetectionResult;
import hma.monitor.strategy.detect.MultipleCyclesCompoundTypeAnomalyDetector;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import hma.util.InspectionDataInjector;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * @author guoyezhi
 *
 */
public class HMA_HDFS_LiveDatanodeHeartBeatIntervalDetector extends
		MultipleCyclesCompoundTypeAnomalyDetector {
	
	private static SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 */
	public HMA_HDFS_LiveDatanodeHeartBeatIntervalDetector(
			String monitorItemName, int cycles) {
		super(monitorItemName, cycles);
	}
	
	
	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.MultipleCyclesCompoundTypeAnomalyDetector#visitMultipleCyclesMonitorStrategyDataCollection(java.util.NavigableMap)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected AnomalyDetectionResult visitMultipleCyclesMonitorStrategyDataCollection(
			NavigableMap<Date, MonitorStrategyDataCollection> multipleCyclesDataCollection) {
		
		final String itemName = "HDFS_RPC_LiveDatanodes_LastContact";
		if (!getMonitorItemName().equals(itemName)) {
			throw new RuntimeException("Configuration Error");
		}
		
		if (multipleCyclesDataCollection.size() < this.getCycles()) {
			return null;
		}
		
		StringBuilder detectionInfoBuilder = new StringBuilder();
		MonitorStrategyData<Map<String, Long>> lastStrategyData = 
			(MonitorStrategyData<Map<String, Long>>) 
			multipleCyclesDataCollection.lastEntry().getValue().get(itemName);
		if (lastStrategyData == null || lastStrategyData.getData() == null) {
			return null;
		}
		detectionInfoBuilder.append(
				"[" + dateFormat.format(multipleCyclesDataCollection.lastKey()) + 
				"] Live Datanode HeartBeat Interval Status List :\n");
		Iterator<Map.Entry<String, Long>> iter = 
			lastStrategyData.getData().entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Long> entry = iter.next();
			String node = entry.getKey();
			long interval = entry.getValue();
			detectionInfoBuilder.append("\t" + node + " -> " + interval + "\n");
		}
		String detectionInfo = detectionInfoBuilder.toString();
		
		Configuration conf = MonitorManager.getGlobalConf();
		long hbIntervalThres = conf.getLong(
				"datanode.heartbeat.interval.alarm.threshold", 
				60);
		double incidenceThres = conf.getFloat(
				"heartbeat.interval.anomaly.incidence.alarm.threshold", 
				(float) 50.0);
		int dnCntWarningThres = conf.getInt(
				"heartbeat.interval.anomalous.datanode.count.warning.level.alarm.threshold", 
				10);
		int dnCntCriticalThres = conf.getInt(
				"heartbeat.interval.anomalous.datanode.count.critical.level.alarm.threshold", 
				40);
		
		int i = this.getCycles();
		Date d = multipleCyclesDataCollection.lastKey();
		Map<String, Integer> counter = new HashMap<String, Integer>();
		while ((i > 0) && (d != null)) {
			
			MonitorStrategyData<?> strategyData = 
				multipleCyclesDataCollection.get(d).get(itemName);
			if (strategyData == null || strategyData.getData() == null) {
				continue;
			}
			Map<String, Long> toDetect = 
				(Map<String, Long>) strategyData.getData();
			
			for (String node : toDetect.keySet()) {
				Long interval = toDetect.get(node);
				if (interval == null) continue;
				if (interval >= hbIntervalThres) {
					Integer occurences = counter.get(node);
					if (occurences == null) {
						counter.put(node, 1);
					} else {
						counter.put(node, occurences + 1);
					}
				}
			}
			
			i--;
			d = multipleCyclesDataCollection.lowerKey(d);
			
		}
		
		boolean anomalous = false;
		int alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		String alarmInfo = null;
		StringBuilder alarmInfoBuilder = new StringBuilder();
		Map<String, Integer> detectionResult = new HashMap<String, Integer>();
		
		if ((i == 0) && (counter.size() > 0)) {
			
			for (String node : counter.keySet()) {
				int occurences = counter.get(node);
				double incidence = 
					100.0 * occurences / this.getCycles();
				if (incidence > incidenceThres) {
					detectionResult.put(node, occurences);
				}
			}
			
			if (detectionResult.size() >= dnCntCriticalThres) {
				anomalous = true;
				alarmLevel =
					MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
			} else if (detectionResult.size() >= dnCntWarningThres) {
				anomalous = true;
				alarmLevel =
					MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			} else if (detectionResult.size() > 0) {
				anomalous = true;
				alarmLevel =
					MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_INFO;
			}
			
			if (anomalous) {
				alarmInfoBuilder.append(
						"心跳间隔过高 DN 总数：" + detectionResult.size() + "\n");
				for (String anomaly : detectionResult.keySet()) {
					long interval = lastStrategyData.getData().get(anomaly);
					alarmInfoBuilder.append("\t" + anomaly + " -> " + interval + "\n");
				}
				alarmInfo = alarmInfoBuilder.toString();
			}
		}

                String clusterName = MonitorManager.getMonitoredClusterName();
	        String strategyName = multipleCyclesDataCollection.lastEntry().getValue().getStrategyName();
	        String detectionTimestamp = dateFormat.format(new Date());
	        String infoLevel = MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(
	                        alarmLevel);
	        String infoContent = detectionResult.size() + "";
	        

	        try {
	                        new InspectionDataInjector(clusterName,strategyName,detectionTimestamp,infoLevel,infoContent);
	                } catch (SQLException e) {
	                        // TODO Auto-generated catch block
	                        e.printStackTrace();
	                } catch (ParseException e) {
	                        // TODO Auto-generated catch block
	                        e.printStackTrace();
	                }
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, detectionInfo);
		
	}
	
}
