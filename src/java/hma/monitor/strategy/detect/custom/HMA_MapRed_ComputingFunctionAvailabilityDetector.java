/**
 * 
 */
package hma.monitor.strategy.detect.custom;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;
import hma.monitor.collection.custom.MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker.HMAProbingJobResult;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.detect.AnomalyDetectionResult;
import hma.monitor.strategy.detect.MultipleCyclesCompoundTypeAnomalyDetector;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;

/**
 * @author guoyezhi
 *
 */
public class HMA_MapRed_ComputingFunctionAvailabilityDetector extends
        MultipleCyclesCompoundTypeAnomalyDetector {
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	/**
	 * @param monitorItemName
	 * @param cycles
	 */
	public HMA_MapRed_ComputingFunctionAvailabilityDetector(
			String monitorItemName, int cycles) {
		super(monitorItemName, cycles);
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	protected AnomalyDetectionResult visitMultipleCyclesMonitorStrategyDataCollection(
			NavigableMap<Date, MonitorStrategyDataCollection> multipleCyclesDataCollection) {
		
		final String jobItemName = 
			"MapRed_RPC_ComputingStatus_AvailabilityProbingJob";
		
		if (multipleCyclesDataCollection.size() < this.getCycles()) {
			return null;
		}
		
		Configuration conf = MonitorManager.getGlobalConf();
		int involvedCycles = conf.getInt("probing.job.involved.cycle.num", 10);
		if (this.getCycles() < involvedCycles) {
			throw new RuntimeException("Configuration Error");
		}
		
		HMAProbingJobResult jobResult = null;
		StringBuilder alarmInfoBuilder = new StringBuilder();
		StringBuilder detectionInfoBuilder = new StringBuilder();
        String PhyQueueName = MonitorManager.getGlobalConf().get("mapred.abaci.phyqueuename", "null");
        List<String> queueList = new ArrayList<String>();
		if (!PhyQueueName.equals("null")){
			String QueueNames [] = PhyQueueName.split(";");
			for(String queueName:QueueNames){
				queueList.add(jobItemName + ":" + queueName);
			}
		}
		queueList.add(jobItemName);
		
	    boolean anomalous = false;
		int alarmLevel = 
			    MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
		for (int i = 0; i < queueList.size(); i++ ){
			 String nowJobItemName = queueList.get(i);
			 //System.out.println("kain's log about mutiqueue monitor, nowQueueName is : " + nowJobItemName);
			
		    for (Date d : multipleCyclesDataCollection.keySet()) {
			    MonitorStrategyData<HMAProbingJobResult> strategyData =
				    (MonitorStrategyData<HMAProbingJobResult>)
				    multipleCyclesDataCollection.get(d).get(nowJobItemName);
			        
			    if (strategyData == null || strategyData.getData() == null) {
				    continue;
			    }
			    HMAProbingJobResult tmp = strategyData.getData();
			    if (jobResult == null || 
					    jobResult.getJobSubmitTime().before(tmp.getJobSubmitTime())) {
				/*
				 * pick up the latest submitted job
				 */
				    jobResult = tmp;
			    }
		    }
		
		 
		
		    detectionInfoBuilder.append( 
			    "[" + dateFormat.format(multipleCyclesDataCollection.lastKey()) + "] " + 
			    "Job: " + jobResult.getJobID() + "\n\t" + 
			    "Name: " + jobResult.getJobName() + "\n\t" + 
			    "Queue: " + jobResult.getQueueName() + "\n\t" + 
			    "Successful: " + jobResult.isSuccessful() + "\n\t" + 
			    "Submit Time: " + jobResult.getJobSubmitTime() + "\n\t" +
			    "Complete Time: " + jobResult.getJobCompleteTime() + "\n" );
		
		    long currTimestamp = System.currentTimeMillis();
		    long jobSubmitTime = jobResult.getJobSubmitTime().getTime();
		    long timeoutWarningThres = 1000 * conf.getLong(
				    "probing.job.timeout.warning.level.alarm.threshold", 
				   600);
		    long timeoutCriticalThres = 1000 * conf.getLong(
				    "probing.job.timeout.critical.level.alarm.threshold", 
				    900);
		    if (currTimestamp - jobSubmitTime >= timeoutWarningThres) {
			
			    if (currTimestamp - jobSubmitTime >= timeoutCriticalThres) {
				    anomalous = true;
				    alarmLevel = 
					    MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
			    } else {
				    anomalous = true;
				    alarmLevel = 
					    MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_WARN;
			    }
			
			alarmInfoBuilder.append("Queue:" + jobResult.getQueueName() +" Probing Job Timeout!\n");
			
			alarmInfoBuilder.append(
				    "\t" + 
				    "Job: " + jobResult.getJobID() + "\n\t" +
				    "Name: " + jobResult.getJobName() + "\n\t" + 
				    "Queue: " + jobResult.getQueueName() + "\n\t" + 
				    "Successful: " + jobResult.isSuccessful() + "\n\t" + 
				    "Submit Time: " + jobResult.getJobSubmitTime() + "\n\t" +
				    "Complete Time: " + jobResult.getJobCompleteTime() + "\n");
			
		    } else if (!jobResult.isSuccessful()) {
			    anomalous = true;
			    alarmLevel = 
				    MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
			    alarmInfoBuilder.append( 
			        "Queue:" + jobResult.getQueueName() + " " +
				    "Probing Job Failed!\n\t" +  
				    "Job: " + jobResult.getJobID() + "\n\t" + 
				    "Name: " + jobResult.getJobName() + "\n\t" + 
				    "Queue: " + jobResult.getQueueName() + "\n\t" + 
				    "Successful: " + jobResult.isSuccessful() + "\n\t" + 
				    "Submit Time: " + jobResult.getJobSubmitTime() + "\n\t" +
				    "Complete Time: " + jobResult.getJobCompleteTime() + "\n");
		    }
		}
		
		String alarmInfo = alarmInfoBuilder.toString().trim() + "\n" ;
		String detectionInfo = detectionInfoBuilder.toString();
		
		return new AnomalyDetectionResult(
				anomalous, alarmLevel, alarmInfo, detectionInfo);
		
	}
	
}
