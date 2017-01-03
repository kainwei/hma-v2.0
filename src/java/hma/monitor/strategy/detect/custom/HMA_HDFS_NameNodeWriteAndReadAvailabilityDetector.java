
package hma.monitor.strategy.detect.custom;

import hma.monitor.MonitorManager;
import hma.monitor.collection.custom.HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm_MonitorItemWorker.BenchmarkOP;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.detect.AnomalyDetectionResult;
import hma.monitor.strategy.detect.SingleCycleSingleStringAnomalyDetector;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author kain
 *
 */
public class HMA_HDFS_NameNodeWriteAndReadAvailabilityDetector extends
        SingleCycleSingleStringAnomalyDetector {

    private static SimpleDateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     * @param monitorItemName
     * @param pattern
     */
    public HMA_HDFS_NameNodeWriteAndReadAvailabilityDetector(String monitorItemName,
                                                             String pattern) {
        super(monitorItemName, pattern);
    }


    /* (non-Javadoc)
     * @see hma.monitor.strategy.detect.SingleCycleSingleTypeAnomalyDetector#visitMonitorStrategyData(hma.monitor.strategy.MonitorStrategyData)
     */
    @Override
    protected AnomalyDetectionResult visitMonitorStrategyData(
            MonitorStrategyData<String> strategyData) {

        final String itemName = "HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm";
        if (!getMonitorItemName().equals(itemName)) {
            throw new RuntimeException("Configuration Error");
        }

        if (strategyData == null ||
                (strategyData.getData() == null)) {
            return null;
        }
        System.out.println("kain's log write readwrite strategy enter");
        Object tmpObject = (Object)strategyData.getData();
        Map<BenchmarkOP,Boolean> resMap = (Map<BenchmarkOP,Boolean>) tmpObject;
        long timestamp = strategyData.getTimestamp();

        boolean anomalous = false;
        int alarmLevel =
                MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_DETECTION_PASSED;
        String alarmInfo = null;
        String dectionInfo =
                "[" + dateFormat.format(new Date(timestamp)) + "] " +
                        getMonitorItemName() + " :\n" ;

        long threshold =
                MonitorManager.getGlobalConf().getLong(
                        "namenode.rpc.timeout.alarm.threshold", 300000);
        long currentTime = System.currentTimeMillis();
        anomalous = ((currentTime - timestamp) > threshold);
        System.out.println("kain's log write readwrite strategy anomlous val " + anomalous );
        if (anomalous) {
            alarmLevel =
                    MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
            alarmInfo =
                    "HDFS服务不可用" + ((currentTime - timestamp) / 1000) + "秒！";
        } else {
            String problemOP = "";
            for(Map.Entry<BenchmarkOP, Boolean>
                    opEntry:resMap.entrySet()	){
                System.out.println("kain's log write readwrite strategy " + opEntry.getKey().toString() + " " + opEntry.getValue());
                if(!opEntry.getValue()){
                    anomalous = true;

                    switch(opEntry.getKey()){
                        case mkdir:
                            problemOP = "建立目录";
                            break;
                        case putFile:
                            problemOP = "上传文件";
                            break;
                        case catFile:
                            problemOP = "cat文件";
                            break;
                        case rmAllB:
                            problemOP = "删除文件";
                            break;

                    }


                }
            }

            if (anomalous) {
                alarmLevel =
                        MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
                alarmInfo =
                        problemOP + "功能不可用！\n" +
                                getMonitorItemName() +
                                " (Collected at: " +
                                dateFormat.format(new Date(timestamp)) +
                                ") :\n" ;
            }
        }

        return new AnomalyDetectionResult(
                anomalous, alarmLevel, alarmInfo, dectionInfo);

    }


}
