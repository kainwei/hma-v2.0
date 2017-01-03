/**
 * 
 */
package hma.monitor.strategy.trigger;

import org.w3c.dom.Element;

import hma.monitor.strategy.MonitorStrategyWorker;

/**
 * @author guoyezhi
 *
 */
public class NeverTypeTaskTrigger implements TaskTrigger {

	/**
	 * 
	 */
	public NeverTypeTaskTrigger(Element conf) {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.trigger.TaskTrigger#visitMonitorStrategyWorker(hma.monitor.strategy.MonitorStrategyWorker)
	 */
	@Override
	public boolean visitMonitorStrategyWorker(MonitorStrategyWorker worker) {
		return false; // always return false
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
