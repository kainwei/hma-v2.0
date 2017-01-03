/**
 * 
 */
package hma.monitor.strategy.trigger;

import hma.monitor.strategy.MonitorStrategyWorker;

/**
 * @author guoyezhi
 *
 */
public interface TaskTrigger {
	
	boolean visitMonitorStrategyWorker(MonitorStrategyWorker worker);
	
}
