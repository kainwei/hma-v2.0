package hma.util;

import hma.monitor.MonitorManager;
import hma.monitor.collection.BuiltinMonitorItemPrototype;

import java.util.Map;

public class BuiltinPrototypesInjector {
    public static void BuiltinPrototypesInjector(Map<String, BuiltinMonitorItemPrototype> NowBuiltinPrototypes) {

        String DstName = "MapRed_RPC_ComputingStatus_AvailabilityProbingJob";
        String PhyQueueName = MonitorManager.getGlobalConf().get("mapred.abaci.phyqueuename", "null");

        if (PhyQueueName.equals("null") || PhyQueueName.equals("")) {
            return;
        }

        if(PhyQueueName != null ){
            String QueueNames[] = PhyQueueName.split(";");

            BuiltinMonitorItemPrototype prototype = NowBuiltinPrototypes.get(DstName);

            for (String QueueName : QueueNames) {

                BuiltinMonitorItemPrototype prototypetmp =
                        new BuiltinMonitorItemPrototype(DstName + ":" + QueueName,
                                prototype.getType(), prototype.getPeriod(), prototype.getDescription(),
                                prototype.getMonitorItemWorkerClass().getName(),
                                prototype.getRawDataTypeName() + "." + QueueName);

                NowBuiltinPrototypes.put(DstName + ":" + QueueName, prototypetmp);
            }
        }




        //Iterator<Map.Entry<String, BuiltinMonitorItemPrototype>> iter = NowBuiltinPrototypes.entrySet().iterator();
        //	while (iter.hasNext()) {
        //		Map.Entry<String, BuiltinMonitorItemPrototype> entry = iter.next();
        //		String name = entry.getKey();
        //System.out.println("kain's buildin item : " + name);
        //	}
        return;


    }
}

