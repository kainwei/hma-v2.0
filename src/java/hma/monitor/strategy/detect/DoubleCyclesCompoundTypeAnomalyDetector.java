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
public abstract class DoubleCyclesCompoundTypeAnomalyDetector extends
		DoubleCyclesAnomalyDetector {

	/**
	 * @param monitorItemName
	 */
	public DoubleCyclesCompoundTypeAnomalyDetector(String monitorItemName) {
		super(monitorItemName);
	}
	
	abstract protected AnomalyDetectionResult visitDoubleCyclesMonitorStrategyDataCollection(
			NavigableMap<Date, MonitorStrategyDataCollection> multipleCyclesDataCollection);

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
			return this.visitDoubleCyclesMonitorStrategyDataCollection(
					strategy.retrieveMultipleCyclesStrategyDataCollection(
							acquisitionTime, DOUBLE_CYCLES_CONSTANT));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
