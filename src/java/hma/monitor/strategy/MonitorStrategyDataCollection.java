/**
 * 
 */
package hma.monitor.strategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * support mapping M items to ONE strategy
 * 
 * @author guoyezhi
 *
 */
public class MonitorStrategyDataCollection {
	
	private String strategyName;
	
	private Map<String, MonitorStrategyData<?>> collection = 
		new HashMap<String, MonitorStrategyData<?>>();
	
	private boolean anomalous = false;
	
	
	public MonitorStrategyDataCollection(String strategyName) {
		this.strategyName = strategyName;   
	}
	
	public Set<String> getInvolvedMonitorItemNames() {
		return this.collection.keySet();
	}
	
	public MonitorStrategyData<?> put(String itemName, MonitorStrategyData<?> data) {
		return this.collection.put(itemName, data);
	}
	
	public MonitorStrategyData<?> get(String itemName) {
		return this.collection.get(itemName);
	}
	
	/**
	 * @param strategyName the strategyName to set
	 */
	public void setStrategyName(String strategyName) {
		this.strategyName = strategyName;
	}
	
	/**
	 * @return the strategyName
	 */
	public String getStrategyName() {
		return strategyName;
	}
	
	/**
	 * @param anomalous the anomalous to set
	 */
	public void setAnomalous(boolean anomalous) {
		this.anomalous = anomalous;
	}

	/**
	 * @return the anomalous
	 */
	public boolean isAnomalous() {
		return anomalous;
	}
	
}
