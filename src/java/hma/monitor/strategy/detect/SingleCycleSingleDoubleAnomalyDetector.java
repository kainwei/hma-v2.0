/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class SingleCycleSingleDoubleAnomalyDetector extends
		SingleCycleSingleTypeAnomalyDetector<Double> {
	
	private double threshold = 0.0;
	
	/**
	 * 
	 * @param monitorItemName
	 * @param threshold
	 */
	public SingleCycleSingleDoubleAnomalyDetector(String monitorItemName,
			double threshold) {
		super(monitorItemName);
		this.setThreshold(threshold);
	}
	
	/**
	 * @param threshold the threshold to set
	 */
	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}
	
	/**
	 * @return the threshold
	 */
	public double getThreshold() {
		return threshold;
	}
	
}
