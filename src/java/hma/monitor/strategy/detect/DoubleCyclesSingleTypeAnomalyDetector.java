/**
 * 
 */
package hma.monitor.strategy.detect;

import java.util.Date;
import java.util.NavigableMap;

import hma.monitor.MonitorStrategy;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.MonitorStrategyWorker;

/**
 * @author guoyezhi
 *
 */
public abstract class DoubleCyclesSingleTypeAnomalyDetector<TYPE> extends
		DoubleCyclesAnomalyDetector {
	
	/**
	 * @param monitorItemName
	 */
	public DoubleCyclesSingleTypeAnomalyDetector(String monitorItemName) {
		super(monitorItemName);
	}
	
	abstract protected AnomalyDetectionResult visitDoubleCyclesMonitorStrategyData(
			NavigableMap<Date, MonitorStrategyData<TYPE>> doubleCyclesData);
	
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
			return this.visitDoubleCyclesMonitorStrategyData(
					strategy.<TYPE>retrieveMultipleCyclesStrategyData(
							this.getMonitorItemName(),
							acquisitionTime,
							DOUBLE_CYCLES_CONSTANT));
		}
		
	}
	
}
