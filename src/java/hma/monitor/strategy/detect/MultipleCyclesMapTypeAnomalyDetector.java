/**
 * 
 */
package hma.monitor.strategy.detect;

import hma.monitor.MonitorStrategy;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.MonitorStrategyWorker;

import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @author guoyezhi
 *
 */
public abstract class MultipleCyclesMapTypeAnomalyDetector<TYPE> extends
		MultipleCyclesAnomalyDetector {

	public MultipleCyclesMapTypeAnomalyDetector(String monitorItemName,
			int cycles) {
		super(monitorItemName, cycles);
	}
	
	abstract protected AnomalyDetectionResult visitMultipleCyclesMonitorStrategyData(
			NavigableMap<Date, MonitorStrategyData<Map<String, TYPE>>> multipleCyclesData);

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
			return this.visitMultipleCyclesMonitorStrategyData(
					strategy.<Map<String, TYPE>>retrieveMultipleCyclesStrategyData(
							getMonitorItemName(),
							acquisitionTime,
							getCycles()));
		}
		
	}
	
}
