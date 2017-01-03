/**
 * 
 */
package hma.monitor.strategy.trigger;

import java.util.Date;
import java.util.SortedMap;

import org.w3c.dom.Element;

import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.MonitorStrategyWorker;

/**
 * @author guoyezhi
 *
 */
public class EverytimeTypeTaskTrigger implements TaskTrigger {
	
	public EverytimeTypeTaskTrigger(Element element) {
		// nothing needed to do
	}

	/* (non-Javadoc)
	 * @see hma.monitor.strategy.trigger.TaskTrigger#visitMonitorStrategyWorker(hma.monitor.strategy.MonitorStrategyWorker)
	 */
	@Override
	public boolean visitMonitorStrategyWorker(MonitorStrategyWorker worker) {
		
		SortedMap<Date, MonitorStrategyDataCollection> dataProcessingCentre = 
			worker.getStrategy().getDataProcessingCentre();
		Date timestamp = worker.getDataAcquisitionTimepoint();
		MonitorStrategyDataCollection dataCollection = 
			dataProcessingCentre.get(timestamp);
		return dataCollection.isAnomalous();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
