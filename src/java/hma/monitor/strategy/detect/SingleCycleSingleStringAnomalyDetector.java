/**
 * 
 */
package hma.monitor.strategy.detect;


/**
 * @author guoyezhi
 *
 */
public abstract class SingleCycleSingleStringAnomalyDetector extends
		SingleCycleSingleTypeAnomalyDetector<String> {
	
	private String pattern = null;

	/**
	 * @param monitorItemName
	 * @param pattern
	 */
	public SingleCycleSingleStringAnomalyDetector(String monitorItemName,
			String pattern) {
		super(monitorItemName);
		this.setPattern(pattern);
	}

	/**
	 * @param pattern the pattern to set
	 */
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	/**
	 * @return the pattern
	 */
	public String getPattern() {
		return pattern;
	}
	
}
