/**
 * 
 */
package hma.monitor.strategy.detect;

import hma.monitor.strategy.MonitorStrategyWorker;

/**
 * Note: The whole module hierarchy is designed under <b>Strategy</b> 
 * Design Pattern.
 * 
 * @author guoyezhi
 *
 */
public interface AnomalyDetector {
	
	/**
	 * If return <b>null</b>, it indicates that the detection result is 
	 * not needed.
	 * 
	 * @param worker
	 * @return
	 */
	AnomalyDetectionResult detect(MonitorStrategyWorker worker);
	
}
