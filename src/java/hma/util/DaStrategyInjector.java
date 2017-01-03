package hma.util;

import hma.monitor.MonitorManager;
import hma.monitor.strategy.MonitorStrategyPrototype;
import hma.monitor.strategy.MonitorStrategyPrototype.DataAcquisitionStrategy;

import java.util.Map;

public class DaStrategyInjector {
	public static void DaStrategyInjector(Map<String, MonitorStrategyPrototype> nowStrategyPrototypes){
		String dstStrategyName = "MapRed_ComputingFunctionAvailability_MON";
		String dstDaStrategyMonitorName = "MapRed_RPC_ComputingStatus_AvailabilityProbingJob";
		
		String PhyQueueName = MonitorManager.getGlobalConf().get("mapred.abaci.phyqueuename", "null");
		
		if (PhyQueueName.equals("null")){
			return;
		}
		String QueueNames [] = PhyQueueName.split(";");

		if(dstStrategyName!=null){
			DataAcquisitionStrategy dstDaStrategy = nowStrategyPrototypes.get(dstStrategyName).getDataAcquisitionStrategy();
			for (String QueueName : QueueNames) {
				dstDaStrategy.putMeta(dstDaStrategyMonitorName  + ":" + QueueName,
						dstDaStrategy.getTimeoutMetaByMonitorItemName(dstDaStrategyMonitorName),
						dstDaStrategy.getAttachedTaskConfsByMonitorItemName(dstDaStrategyMonitorName));

			}
		}
		


		
//		for ( String daName:nowStrategyPrototypes.get(dstStrategyName).getDataAcquisitionStrategy().getAllInvolvedItemNames() ){
//		System.out.println("kain's log from DaStrategyInjector daStrategyName is : " + daName);
//	    }
	}
}
