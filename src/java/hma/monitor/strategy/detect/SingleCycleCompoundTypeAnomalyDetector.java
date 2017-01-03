/**
 * 
 */
package hma.monitor.strategy.detect;

import hma.monitor.MonitorStrategy;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.MonitorStrategyWorker;

import java.util.Date;
import java.util.NavigableMap;

/**
 * @author guoyezhi
 *
 */
public abstract class SingleCycleCompoundTypeAnomalyDetector extends
		SingleCycleAnomalyDetector {
	
	/**
	 * @param monitorItemName
	 */
	public SingleCycleCompoundTypeAnomalyDetector(String monitorItemName) {
		super(monitorItemName);
	}
	
	abstract protected AnomalyDetectionResult visitMonitorStrategyDataCollection(
			MonitorStrategyDataCollection strategyDataCollection);
	
	/* (non-Javadoc)
	 * @see hma.monitor.strategy.detect.AnomalyDetector#detect(hma.monitor.strategy.MonitorStrategyWorker)
	 */
	@Override
	public AnomalyDetectionResult detect(MonitorStrategyWorker worker) {
		
		MonitorStrategy strategy = worker.getStrategy();
		NavigableMap<Date, MonitorStrategyDataCollection> dataProcessingCentre =
			strategy.getDataProcessingCentre();
		
		synchronized (dataProcessingCentre) {
			Date acquisitionTime = worker.getDataAcquisitionTimepoint();
			return this.visitMonitorStrategyDataCollection(
					strategy.retrieveSingleCycleStrategyDataCollection(acquisitionTime));
		}
		
	}
	
}
