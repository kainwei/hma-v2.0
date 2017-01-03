/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class SingleCycleAnomalyDetector implements AnomalyDetector {
	
	private String monitorItemName = null;
	
	/**
	 * @param monitorItemName
	 */
	public SingleCycleAnomalyDetector(String monitorItemName) {
		this.setMonitorItemName(monitorItemName);
	}
	
	/**
	 * @param monitorItemName the monitorItemName to set
	 */
	public void setMonitorItemName(String monitorItemName) {
		this.monitorItemName = monitorItemName;
	}
	
	/**
	 * @return the monitorItemName
	 */
	public String getMonitorItemName() {
		return monitorItemName;
	}
	
}
