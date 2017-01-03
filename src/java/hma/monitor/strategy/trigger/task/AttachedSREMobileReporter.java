/**
 * 
 */
package hma.monitor.strategy.trigger.task;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;
import hma.web.HMAJspHelper;

import java.text.SimpleDateFormat;

/**
 * @author guoyezhi
 *
 */
public class AttachedSREMobileReporter extends MonitorStrategyAttachedTask {
	
	private String[] servers = null;
	
	private String hmaGSMsend = null;
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * @param taskWorker
	 * @param workerArgs
	 * @param args
	 */
	public AttachedSREMobileReporter(String taskWorker, String[] workerArgs,
			MonitorStrategyAttachedTaskArguments args) {
		super(taskWorker, workerArgs, args);
		this.servers = 
			MonitorManager.getGlobalConf().getStrings("baidu.gsm.servers");
		this.hmaGSMsend = 
			System.getProperty("hma.home.dir") + "/conf/" + 
			MonitorManager.getMonitoredClusterName() + "/gsmsend.hma";
	}
	
	private void doSendSM(String[] receivers, String message) {
		
		Runtime rt = Runtime.getRuntime();

		for (int i = 0; i < receivers.length; i++) {
			for (int j = 0; j < servers.length; j++) {
				try {
					for (String str : new String[] {
							"gsmsend", "-s", servers[j].trim(),
							receivers[i].trim() + "@" + '"' + message + '"' }) {
						System.out.print(str + " ");
					}
					System.out.println();

					String [] smscommand = new String[] {
							hmaGSMsend,
							"-s", servers[j].trim(),
							receivers[i].trim(),
							message };
					rt.exec(smscommand);
					for(String str:smscommand){
						
						System.out.println("kain's sms log "+  str );
					}

					
					break;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		
		MonitorStrategyAttachedTaskArguments taskArgs = 
			getAttachedTaskArguments();
		String monitorStrategyName = 
			taskArgs.getMonitorStrategyName();
		String alarmLevel = taskArgs.getAlarmLevel();
		
		Configuration conf = MonitorManager.getGlobalConf();
		String alarmThres = conf.get(
				"mobile.reporter.alarm.level.threshold", 
				"NOTICE");
		
		if (MonitorStrategyAttachedTaskAdapter.compareAlarmLevel(
				alarmLevel, alarmThres) <= 0) {
			return;
		}
		
		String alarmTag = getAlarmTag(monitorStrategyName, alarmLevel);
		String[] receivers = getReceivers(alarmLevel, conf);
		
		StringBuilder msgBuilder = new StringBuilder();
		msgBuilder.append("[HMA]");
		msgBuilder.append("[" + taskArgs.getTargetSystemName() + "]");
		msgBuilder.append("[" + alarmTag + "]");
		msgBuilder.append("[" + HMAJspHelper.getMonitorStrategyAlias(monitorStrategyName) + "]");
		msgBuilder.append("[" + taskArgs.getKeyInfo() + "]");
		msgBuilder.append("[" + dateFormat.format(taskArgs.getTimestamp()) + "]");
		
		doSendSM(receivers, msgBuilder.toString());
		
	}
	
	private String getAlarmTag(String monitorStrategyName, String alarmLevel) {
		String alarmTag = null;
		if (monitorStrategyName.equals("INode Quota") || 
				monitorStrategyName.equals("Space Quota")) {
			alarmTag = alarmLevel;
		} else {
			if (MonitorStrategyAttachedTaskAdapter.compareAlarmLevel(
					alarmLevel, "WARN") == 0) {
				alarmTag = "WARN";
			} else if (MonitorStrategyAttachedTaskAdapter.compareAlarmLevel(
					alarmLevel, "CRITICAL") >= 0) {
				alarmTag = "CRITICAL";
			} else {
				alarmTag = "CRITICAL";
			}
		}
		return alarmTag;
	}
	
	private String[] getReceivers(String infoLevel, Configuration conf) {
		String[] receivers = null;
		if (MonitorStrategyAttachedTaskAdapter.compareAlarmLevel(
				infoLevel, "WARN") == 0) {
			receivers = conf.getStrings(
					"sre.mobile.reporter.warning.level.alarm.receivers");
		} else if (MonitorStrategyAttachedTaskAdapter.compareAlarmLevel(
				infoLevel, "CRITICAL") >= 0) {
			receivers = conf.getStrings(
					"sre.mobile.reporter.critical.level.alarm.receivers");
		} else {
			receivers = getWorkerArgs();
		}
		return receivers;
	}
	
}

