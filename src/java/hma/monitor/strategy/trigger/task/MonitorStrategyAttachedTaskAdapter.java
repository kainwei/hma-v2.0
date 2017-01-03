/**
 * 
 */
package hma.monitor.strategy.trigger.task;

import java.util.Date;

/**
 * @author guoyezhi
 *
 */
public class MonitorStrategyAttachedTaskAdapter {
	
	private final Class<MonitorStrategyAttachedTask> taskType;
	
	private int continuousOccurrence = -1;
	
	private final int continuousLimit;
	
	/*
	 * added by Jiangtao02
	 */
	private final long intervalLimit;
	private long intervalOld = -1;
	
	private final String taskWorker;
	
	private String[] workerArgs;
	
	private MonitorStrategyAttachedTaskArguments lastTaskArgs = null;
	
	private MonitorStrategyAttachedTaskArguments lastTriggeredTaskArgs = null; 
	
	
	/**
	 * @param continuousLimit
	 */
	public MonitorStrategyAttachedTaskAdapter(
			Class<MonitorStrategyAttachedTask> taskType,
			int continuousLimit,
			String taskWorker,
			String workerArgsConf,
			long intervalLimit) {
		this.taskType = taskType;
		this.continuousOccurrence = 0;
		this.continuousLimit = continuousLimit;
		this.taskWorker = taskWorker;
		if (workerArgsConf == null) {
			this.workerArgs = null;
		} else {
			this.workerArgs = workerArgsConf.split(",");
		}
		this.intervalLimit = intervalLimit;
	}
	
	
	/**
	 * @return the taskWorker
	 */
	public String getTaskWorker() {
		return taskWorker;
	}
	
	/**
	 * @param continuousOccurrence the continuousOccurrence to set
	 */
	public synchronized void setContinuousOccurrence(int continuousOccurrence) {
		this.continuousOccurrence = continuousOccurrence;
	}
	
	/**
	 * @return the continuousOccurrence
	 */
	public synchronized int getContinuousOccurrence() {
		return continuousOccurrence;
	}
	
	public synchronized void clearContinuousOccurrence() {
		
		if (continuousOccurrence > 0 
				&& shouldReportRecovery(lastTriggeredTaskArgs)) {
			
			lastTriggeredTaskArgs.setKeyInfo(
					"恢复正常（异常持续" + continuousOccurrence + "分钟）");
			lastTriggeredTaskArgs.setFullInfo(
					"恢复正常（异常持续" + continuousOccurrence + "分钟）");
			lastTriggeredTaskArgs.setTimestamp(new Date());
			
			MonitorStrategyAttachedTask task = null;
			try {
				task = this.taskType.getConstructor(
						String.class, String[].class, 
						MonitorStrategyAttachedTaskArguments.class).newInstance(
								taskWorker, workerArgs, lastTriggeredTaskArgs);
				new Thread(task).start();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			
		}
		
		continuousOccurrence = 0;
		lastTriggeredTaskArgs = null;
		
	}
	
	private boolean shouldReportRecovery(MonitorStrategyAttachedTaskArguments args) {
		
		/* 
		 * In some cases, not getting worse do not mean recovery, 
		 * e.g. HDFS_DatanodeAliveToDead_MON & MapRed_TasktrackerAliveToDead_MON
		 */
		if (args.getMonitorStrategyName().contains("AliveToDead")) {
			return false;
		}
		
		return true;
		
	}
	
	public synchronized void runTask(MonitorStrategyAttachedTaskArguments taskArgs) {
		
		boolean shouldTrigger = false;
		
		if (continuousLimit <= 0) {
			continuousOccurrence++;
			shouldTrigger = true;
		} else if (continuousLimit > 0) {
			if (lastTaskArgs == null) {
				continuousOccurrence++;
			} else if (taskArgs.getTimestamp().after(
					lastTaskArgs.getTimestamp())) {
				continuousOccurrence++;
			}
			if ((continuousOccurrence > 0) 
					&& (continuousOccurrence <= continuousLimit)) {
				shouldTrigger = true;
				intervalOld = System.currentTimeMillis();
			} else if(continuousOccurrence > 0){
			    //added by jiangtao02
			    if(intervalOld > 0 && intervalLimit > 0){
			        long now = System.currentTimeMillis();
			        if((now - intervalOld) > intervalLimit * 1000) {
			            /*
			             * Debug
			             */
			            System.out.println("##Debug: " + (now - intervalOld) + " < Limit:" + intervalLimit * 1000);
			            shouldTrigger = true;
			            intervalOld = now;
			        }
			    }
			}
		}
		
		
		/*
		 * In case the situation of monitor strategy is getting worse 
		 * after sub-tasks have been triggered more than limit times, 
		 * we add the following codes to support high priority override.
		 */
		if (!shouldTrigger && (lastTriggeredTaskArgs != null)) {
			if (compareAlarmLevel(
					taskArgs.getAlarmLevel(), 
					lastTriggeredTaskArgs.getAlarmLevel()) > 0) {
				continuousOccurrence = 1;
				shouldTrigger = true;
				intervalOld = System.currentTimeMillis() * 1000;
			}
		}
		
		lastTaskArgs = taskArgs;
		
		if (shouldTrigger) {
			MonitorStrategyAttachedTask task = null;
			try {
				task = this.taskType.getConstructor(
						String.class, String[].class, 
						MonitorStrategyAttachedTaskArguments.class).newInstance(
								taskWorker, workerArgs, taskArgs);
				new Thread(task).start();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			lastTriggeredTaskArgs = taskArgs;
		}
		
	}
	
	
	/*
	 * 
	 * 
	 */
	public static final int ALARM_LEVEL_DETECTION_PASSED = 0x0000;
	
	public static final int ALARM_LEVEL_ACCQ_TIMEOUT     = 0x4000;
	
	public static final int ALARM_LEVEL_UNINIT           = 0x8000;
	public static final int ALARM_LEVEL_INFO             = 0x8001;
	public static final int ALARM_LEVEL_NOTICE           = 0x8002;
	public static final int ALARM_LEVEL_WARN             = 0x8003;
	public static final int ALARM_LEVEL_CRITICAL         = 0x8004;
	public static final int ALARM_LEVEL_EMERG            = 0x8005;
	
	public static int compareAlarmLevel(int val1, int val2) {
		if (val1 == val2) {
			return 0;
		} else if (val1 > val2) {
			return 1;
		} else {
			return -1;
		}
	}
	
	public static int compareAlarmLevel(String valStr1, String valStr2) {
		int val1 = getAlarmLevelValueFromString(valStr1);
		int val2 = getAlarmLevelValueFromString(valStr2);
		if (val1 == val2) {
			return 0;
		} else if (val1 > val2) {
			return 1;
		} else {
			return -1;
		}
	}
	
	public static String getAlarmLevelStringFromValue(int val) {
		if (val == ALARM_LEVEL_INFO) {
			return "INFO";
		}
		else if (val == ALARM_LEVEL_NOTICE) {
			return "NOTICE";
		}
		else if (val == ALARM_LEVEL_WARN) {
			return "WARN";
		}
		else if (val == ALARM_LEVEL_CRITICAL) {
			return "CRITICAL";
		}
		else if (val == ALARM_LEVEL_EMERG) {
			return "EMERG";
		}
		else {
			return "UNINIT";
		}
	}
	
	public static int getAlarmLevelValueFromString(String str) {
		if (str.equals("INFO")) {
			return ALARM_LEVEL_INFO;
		}
		else if (str.equals("NOTICE")) {
			return ALARM_LEVEL_NOTICE;
		}
		else if (str.equals("WARN") || str.equals("WARNING")) {
			return ALARM_LEVEL_WARN;
		}
		else if (str.equals("CRITICAL")) {
			return ALARM_LEVEL_CRITICAL;
		}
		else if (str.equals("EMERG")) {
			return ALARM_LEVEL_EMERG;
		}
		else {
			return ALARM_LEVEL_UNINIT;
		}
	}
	
}
