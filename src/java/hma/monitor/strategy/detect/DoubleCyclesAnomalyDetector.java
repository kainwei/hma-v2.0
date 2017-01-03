/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class DoubleCyclesAnomalyDetector implements AnomalyDetector {
	
	private String monitorItemName = null;
	
	protected static final int DOUBLE_CYCLES_CONSTANT = 2;

	/**
	 * @param monitorItemName
	 */
	public DoubleCyclesAnomalyDetector(String monitorItemName) {
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
