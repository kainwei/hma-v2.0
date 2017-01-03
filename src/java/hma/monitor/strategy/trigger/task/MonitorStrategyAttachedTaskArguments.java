/**
 * 
 */
package hma.monitor.strategy.trigger.task;

import java.util.Date;

/**
 * @author guoyezhi
 * add additionalInfo by chiwen01
 *
 */
public class MonitorStrategyAttachedTaskArguments {
	
	private String targetSystemName = null;
	
	private String monitorStrategyName = null;
	
	private String alarmLevel = null;
	
	private String keyInfo = null;
	
	private String fullInfo = null;
	
	private Date timestamp = null;

    private String additionalInfo = null;
	
	
	public MonitorStrategyAttachedTaskArguments(
			String targetSystemName,
			String monitorStrategyName,
			String alarmLevel,
			String keyInfo,
			String fullInfo,
			Date timestamp,
            String additionalInfo) {
		this.targetSystemName = targetSystemName;
		this.monitorStrategyName = monitorStrategyName;
		this.alarmLevel = alarmLevel;
		this.keyInfo = keyInfo;
		this.fullInfo = fullInfo;
		this.timestamp = timestamp;
        this.additionalInfo = additionalInfo;
	}
	
	public MonitorStrategyAttachedTaskArguments(
			String targetSystemName,
			String monitorStrategyName,
			String alarmLevel,
			String keyInfo,
			String fullInfo,
			Date timestamp) {
		this.targetSystemName = targetSystemName;
		this.monitorStrategyName = monitorStrategyName;
		this.alarmLevel = alarmLevel;
		this.keyInfo = keyInfo;
		this.fullInfo = fullInfo;
		this.timestamp = timestamp;
	}
	
	public String getTargetSystemName() {
		return targetSystemName;
	}
	
	public String getMonitorStrategyName() {
		return monitorStrategyName;
	}
	
	public String getAlarmLevel() {
		return alarmLevel;
	}
	
	public String getKeyInfo() {
		return keyInfo;
	}
	
	public void setKeyInfo(String keyInfo) {
		this.keyInfo = keyInfo;
	}
	
	public String getFullInfo() {
		return fullInfo;
	}
	
	public void setFullInfo(String fullInfo) {
		this.fullInfo = fullInfo;
	}
	
	public Date getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo() {
        this.additionalInfo = additionalInfo;
    }
	
}
