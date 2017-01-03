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
import java.util.Set;

import java.sql.SQLException;
import java.text.ParseException;
import hma.util.InspectionDataInjector;
import java.text.DecimalFormat;

/**
 * @author guoyezhi
 *
 */
public class HMA_HDFS_LiveDatanodeXceiverUsageDetector extends
		MultipleCyclesCompoundTypeAnomalyDetector {
	
	private static SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 */
	public HMA_HDFS_LiveDatanodeXceiverUsageDetector(String monitorItemName,
			int cycles) {
		super(monitorItemName, cycles);
	}
	
	
	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.MultipleCyclesCompoundTypeAnomalyDetector#visitMultipleCyclesMonitorStrategyDataCollection(java.util.NavigableMap)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected AnomalyDetectionResult visitMultipleCyclesMonitorStrategyDataCollection(
			NavigableMap<Date, MonitorStrategyDataCollection> multipleCyclesDataCollection) {
		
		final String itemName = "HDFS_RPC_LiveDatanodes_XceiverUsage";
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
				"] Live Datanode Xceiver Usage Status List :\n");
		Iterator<Map.Entry<String, Long>> iter = 
			lastStrategyData.getData().entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Long> entry = iter.next();
			String node = entry.getKey();
			long usage = entry.getValue();
			detectionInfoBuilder.append("\t" + node + " -> " + usage + "\n");
		}
		String detectionInfo = detectionInfoBuilder.toString();
		
		Configuration conf = MonitorManager.getGlobalConf();
		long xceiverUsageThres = conf.getLong(
				"datanode.xceiver.urate.alarm.threshold", 
				600);
		int dnCntWarningThres = conf.getInt(
				"xceiver.urate.anomalous.datanode.count.warning.level.alarm.threshold", 
				10);
		int dnCntCriticalThres = conf.getInt(
				"xceiver.urate.anomalous.datanode.count.critical.level.alarm.threshold", 
				40);
		
		int i = this.getCycles();
		Date d = multipleCyclesDataCollection.lastKey();
		Set<String> anomalies = lastStrategyData.getData().keySet();

        double total = anomalies.size() * 1.0 ;
        boolean slave_rate = conf.getBoolean("slave.rate",true) ;
        float slaveWarnThres = conf.getFloat("dn.xceiver.warn.percent.thres",(float)0.2);
        float slaveCriticalThres = conf.getFloat("dn.xceiver.critical.percent.thres",(float)0.3);
		while ((i > 0) && (d != null) && (anomalies.size() > 0)) {
			
			MonitorStrategyData<?> strategyData = 
				multipleCyclesDataCollection.get(d).get(itemName);
			if (strategyData == null || strategyData.getData() == null) {
				return null;
			}
			Map<String, Long> toDetect = 
				(Map<String, Long>) strategyData.getData();
			Map<String, Long> detected = new HashMap<String, Long>();
			
			Iterator<String> it = anomalies.iterator();
			while (it.hasNext()) {
				String node = it.next();
				Long usage = toDetect.get(node);
				if (usage == null) continue;
				if (usage >= xceiverUsageThres) {
					detected.put(node, usage);
				}
			}
			
			i--;
			d = multipleCyclesDataCollection.lowerKey(d);
			anomalies = detected.keySet();
			
		}
		
		boolean anomalous = false;
		int alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		String alarmInfo = null;
		StringBuilder alarmInfoBuilder = new StringBuilder();

        double  anomalieNum = anomalies.size() * 1.0;
        DecimalFormat df1 = new DecimalFormat("0.00");
        float nownorate = Float.valueOf(df1.format( anomalieNum / total)).floatValue();
      //  System.out.println("masuhua dn xceiver    total:"+total+"  slave_rate:"+slave_rate+" no num:"+anomalieNum+"  rate:"+nownorate+"  slaveThres:"+slaveThres);
		if ((i == 0) && (anomalies.size() > 0)) {
			if ( slave_rate ){
                if ( nownorate >= slaveCriticalThres){
                    alarmLevel =
                            MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
                }else{
                    if(nownorate >= slaveWarnThres) {
                    alarmLevel =
                            MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
                    }else{
                        alarmLevel =
                                MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_INFO;
                    }
                    }
            }else{
                if (  anomalies.size() >= dnCntCriticalThres ){
                    alarmLevel =
                            MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
                } else {
                    alarmLevel =
                            MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_INFO;
                }
            }
			anomalous = true;
			alarmInfoBuilder.append(
					"Xceiver 句柄过高 DN 总数：" + anomalies.size() +  " 占比:" + nownorate+"\n");
			for (String anomaly : anomalies) {
				long usage = lastStrategyData.getData().get(anomaly);
				alarmInfoBuilder.append("\t" + anomaly + " -> " + usage + "\n");
			}
			alarmInfo = alarmInfoBuilder.toString();
		}

        String clusterName = MonitorManager.getMonitoredClusterName();
        String strategyName = multipleCyclesDataCollection.lastEntry().getValue().getStrategyName();
        String detectionTimestamp = dateFormat.format(new Date());
        String infoLevel = MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(
                        alarmLevel);
        String infoContent = anomalies.size() + "";
        //System.out.println ("kain's inspection log: " + clusterName + " " + strategyName + " " + detectionTimestamp + " " + infoLevel + " " + infoContent );

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
