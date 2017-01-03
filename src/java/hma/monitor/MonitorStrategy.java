/**
 * 
 */
package hma.monitor;

import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import hma.monitor.collection.MonitorItemData;
import hma.monitor.strategy.MonitorStrategyData;
import hma.monitor.strategy.MonitorStrategyDataCollection;
import hma.monitor.strategy.MonitorStrategyPrototype;

/**
 * @author guoyezhi
 *
 */
public class MonitorStrategy {
	
	private MonitorStrategyPrototype prototype = null;
	
	private Map<String, MonitorItemData<?>> dataPool = null;
	
	private NavigableMap<Date, MonitorStrategyDataCollection> dataProcessingCentre = null;
	
	public MonitorStrategy(MonitorStrategyPrototype prototype,
			Map<String, MonitorItemData<?>> dataPool) {
		this.setPrototype(prototype);
		this.setDataPool(dataPool);
		this.setDataProcessingCentre(
				new TreeMap<Date, MonitorStrategyDataCollection>());
	}

	/**
	 * @param prototype the prototype to set
	 */
	public void setPrototype(MonitorStrategyPrototype prototype) {
		this.prototype = prototype;
	}

	/**
	 * @return the prototype
	 */
	public MonitorStrategyPrototype getPrototype() {
		return prototype;
	}

	/**
	 * @param dataPool the dataPool to set
	 */
	public void setDataPool(Map<String, MonitorItemData<?>> dataPool) {
		this.dataPool = dataPool;
	}
	
	/**
	 * @return the dataPool
	 */
	public Map<String, MonitorItemData<?>> getDataPool() {
		return dataPool;
	}
	
	/**
	 * @param dataProcessingCentre the dataProcessingCentre to set
	 */
	public void setDataProcessingCentre(NavigableMap<Date, MonitorStrategyDataCollection> dataProcessingCentre) {
		this.dataProcessingCentre = dataProcessingCentre;
	}
	
	/**
	 * @return the dataProcessingCentre
	 */
	public NavigableMap<Date, MonitorStrategyDataCollection> getDataProcessingCentre() {
		return dataProcessingCentre;
	}
	
	@SuppressWarnings("unchecked")
	public synchronized <TYPE> MonitorStrategyData<TYPE> retrieveSingleCycleStrategyData(
			String monitorItemName, Date date) {
		return (MonitorStrategyData<TYPE>) this.dataProcessingCentre.get(date).get(monitorItemName);
	}
	
	public synchronized MonitorStrategyDataCollection retrieveSingleCycleStrategyDataCollection(
			Date date) {
		return this.dataProcessingCentre.get(date);
	}
	
	public synchronized NavigableMap<Date, MonitorStrategyDataCollection>
	retrieveMultipleCyclesStrategyDataCollection(Date cutoff, int lastCycles) {
		if (lastCycles < 1)
			throw new RuntimeException();
		NavigableMap<Date, MonitorStrategyDataCollection> multipleDataCollection =
			new TreeMap<Date, MonitorStrategyDataCollection>();
		Date date = cutoff;
		for (int i = lastCycles; i > 0 && date != null; i--) {
			MonitorStrategyDataCollection dataCollection = 
				this.dataProcessingCentre.get(date);
			multipleDataCollection.put(date, dataCollection);
			date = this.dataProcessingCentre.lowerKey(date);
		}
		if (multipleDataCollection.size() > 0)
			return multipleDataCollection;
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public synchronized <TYPE> NavigableMap<Date, MonitorStrategyData<TYPE>> retrieveMultipleCyclesStrategyData(
			String monitorItemName, Date cutoff, int lastCycles) {
		if (lastCycles < 1)
			throw new RuntimeException();
		NavigableMap<Date, MonitorStrategyData<TYPE>> multipleData =
			new TreeMap<Date, MonitorStrategyData<TYPE>>();
		Date date = cutoff;
		for (int i = lastCycles; i > 0 && date != null; i--) {
			MonitorStrategyData<TYPE> data = 
				(MonitorStrategyData<TYPE>) this.dataProcessingCentre.get(date).get(monitorItemName);
			multipleData.put(date, data);
			date = this.dataProcessingCentre.lowerKey(date);
		}
		if (multipleData.size() != 0)
			return multipleData;
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
