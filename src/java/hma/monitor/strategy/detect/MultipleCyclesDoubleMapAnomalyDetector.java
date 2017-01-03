/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class MultipleCyclesDoubleMapAnomalyDetector extends
		MultipleCyclesMapTypeAnomalyDetector<Double> {
	
	private SingleCycleSingleDoubleAnomalyDetector singleCycleDetector = null;

	/**
	 * @param monitorItemName
	 * @param cycles
	 */
	public MultipleCyclesDoubleMapAnomalyDetector(String monitorItemName,
			int cycles, SingleCycleSingleDoubleAnomalyDetector singleDetector) {
		super(monitorItemName, cycles);
		this.setSingleDetector(singleDetector);
	}
	
	/**
	 * @param singleCycleDetector the singleCycleDetector to set
	 */
	public void setSingleDetector(SingleCycleSingleDoubleAnomalyDetector singleDetector) {
		this.singleCycleDetector = singleDetector;
	}

	/**
	 * @return the singleCycleDetector
	 */
	public SingleCycleSingleDoubleAnomalyDetector getSingleDetector() {
		return singleCycleDetector;
	}
	
}
