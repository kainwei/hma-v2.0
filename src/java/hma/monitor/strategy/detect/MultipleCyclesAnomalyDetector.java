/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class MultipleCyclesAnomalyDetector implements AnomalyDetector {
	
	private String monitorItemName = null;
	
	private int cycles = 0;

	/**
	 * @param monitorItemName
	 * @param cycles
	 */
	public MultipleCyclesAnomalyDetector(String monitorItemName,
			int cycles) {
		this.setMonitorItemName(monitorItemName);
		this.setCycles(cycles);
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
	
	/**
	 * @param cycles the cycles to set
	 */
	public void setCycles(int cycles) {
		this.cycles = cycles;
	}

	/**
	 * @return the cycles
	 */
	public int getCycles() {
		return cycles;
	}
	
}
