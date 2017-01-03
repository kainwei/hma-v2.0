/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class DoubleCyclesSingleLongAnomalyDetector extends
		DoubleCyclesSingleTypeAnomalyDetector<Long> {
	
	private long threshold = 0;

	/**
	 * @param monitorItemName
	 * @param threshold
	 */
	public DoubleCyclesSingleLongAnomalyDetector(String monitorItemName,
			long threshold) {
		super(monitorItemName);
		this.setThreshold(threshold);
	}

	/**
	 * @param threshold the threshold to set
	 */
	public void setThreshold(long threshold) {
		this.threshold = threshold;
	}

	/**
	 * @return the threshold
	 */
	public long getThreshold() {
		return threshold;
	}
	
}
