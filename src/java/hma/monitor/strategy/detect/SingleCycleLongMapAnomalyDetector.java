/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class SingleCycleLongMapAnomalyDetector extends
		SingleCycleMapTypeAnomalyDetector<Long> {
	
	private SingleCycleSingleLongAnomalyDetector singleTypeDetector = null;

	/**
	 * @param monitorItemName
	 */
	public SingleCycleLongMapAnomalyDetector(String monitorItemName,
			SingleCycleSingleLongAnomalyDetector singleTypeDetector) {
		super(monitorItemName);
		this.setSingleTypeDetector(singleTypeDetector);
	}
	
	/**
	 * @param singleTypeDetector the singleTypeDetector to set
	 */
	public void setSingleTypeDetector(SingleCycleSingleLongAnomalyDetector singleTypeDetector) {
		this.singleTypeDetector = singleTypeDetector;
	}
	
	/**
	 * @return the singleTypeDetector
	 */
	public SingleCycleSingleLongAnomalyDetector getSingleTypeDetector() {
		return singleTypeDetector;
	}
	
}
