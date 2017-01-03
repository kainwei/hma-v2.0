/**
 * 
 */
package hma.monitor.strategy.detect;

import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;

/**
 * @author guoyezhi
 * add additionalInfo by chiwen01
 *
 */
public class AnomalyDetectionResult {
	
	private boolean anomalous = true;
	
	private int alarmLevel = 
		MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_UNINIT;
	
	private String alarmInfo = null;
	
	private String dectionInfo = null;

    private String additionalInfo = null;
	
	
	public AnomalyDetectionResult(
			boolean anomalous,
			int alarmLevel,
			String alarmInfo,
			String dectionInfo) {
		this.anomalous = anomalous;
		this.alarmLevel = alarmLevel;
		this.alarmInfo = alarmInfo;
		this.dectionInfo = dectionInfo;
	}
	
	public AnomalyDetectionResult(
			boolean anomalous,
			int alarmLevel,
			String alarmInfo,
			String dectionInfo,
            String additionalInfo) {
		this.anomalous = anomalous;
		this.alarmLevel = alarmLevel;
		this.alarmInfo = alarmInfo;
		this.dectionInfo = dectionInfo;
        this.additionalInfo = additionalInfo;
	}
	
	public boolean isAnomalous() {
		return anomalous;
	}
	
	public int getAlarmLevel() {
		return alarmLevel;
	}
	
	public String getAlarmInfo() {
		return alarmInfo;
	}
	
	public String getDectionInfo() {
		return dectionInfo;
	}
	
	public String getAdditionalInfo() {
		return additionalInfo;
	}
}
