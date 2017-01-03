/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class MultipleCyclesLongMapAnomalyDetector extends
		MultipleCyclesMapTypeAnomalyDetector<Long> {
	
	private SingleCycleSingleLongAnomalyDetector singleCycleDetector = null;

	/**
	 * @param monitorItemName
	 * @param cycles
	 */
	public MultipleCyclesLongMapAnomalyDetector(String monitorItemName,
			int cycles, SingleCycleSingleLongAnomalyDetector singleDetector) {
		super(monitorItemName, cycles);
		this.setSingleDetector(singleDetector);
	}
	
	/**
	 * @param singleCycleDetector the singleCycleDetector to set
	 */
	public void setSingleDetector(SingleCycleSingleLongAnomalyDetector singleDetector) {
		this.singleCycleDetector = singleDetector;
	}

	/**
	 * @return the singleCycleDetector
	 */
	public SingleCycleSingleLongAnomalyDetector getSingleDetector() {
		return singleCycleDetector;
	}
	
}
