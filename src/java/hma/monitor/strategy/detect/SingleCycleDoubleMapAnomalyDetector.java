/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class SingleCycleDoubleMapAnomalyDetector extends
		SingleCycleMapTypeAnomalyDetector<Double> {
	
	private SingleCycleSingleDoubleAnomalyDetector singleTypeDetector = null;
	
	/**
	 * @param monitorItemName
	 */
	public SingleCycleDoubleMapAnomalyDetector(String monitorItemName,
			SingleCycleSingleDoubleAnomalyDetector singleTypeDetector) {
		super(monitorItemName);
		this.setSingleTypeDetector(singleTypeDetector);
	}
	
	/**
	 * @param singleTypeDetector the singleTypeDetector to set
	 */
	public void setSingleTypeDetector(SingleCycleSingleDoubleAnomalyDetector singleTypeDetector) {
		this.singleTypeDetector = singleTypeDetector;
	}
	
	/**
	 * @return the singleTypeDetector
	 */
	public SingleCycleSingleDoubleAnomalyDetector getSingleTypeDetector() {
		return singleTypeDetector;
	}
	
}
