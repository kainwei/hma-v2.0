/**
 * 
 */
package hma.monitor.strategy.detect;

import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;

import hma.monitor.MonitorStrategy;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.MonitorStrategyWorker;

/**
 * @author guoyezhi
 *
 */
public abstract class SingleCycleMapTypeAnomalyDetector<TYPE> extends
		SingleCycleAnomalyDetector {

	/**
	 * @param monitorItemName
	 */
	public SingleCycleMapTypeAnomalyDetector(String monitorItemName) {
		super(monitorItemName);
	}
	
	abstract protected AnomalyDetectionResult visitMonitorStrategyData(
			MonitorStrategyData<Map<String, TYPE>> strategyData);
	
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
			return this.visitMonitorStrategyData(
					strategy.<Map<String, TYPE>>retrieveSingleCycleStrategyData(
							getMonitorItemName(), acquisitionTime));
		}
		
	}
	
}
