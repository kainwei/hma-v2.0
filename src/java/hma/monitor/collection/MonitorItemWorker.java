/**
 * 
 */
package hma.monitor.collection;

/**
 * @author guoyezhi
 * 
 */
public abstract class MonitorItemWorker<RAW, EXTRACTED> {
	
	private MonitorItemPrototype prototype = null;
	
	public abstract MonitorItemData<RAW> collect();
	
	public abstract MonitorItemData<EXTRACTED> extract(MonitorItemData<RAW> rawData);
	
	public void setMonitorItemPrototype(MonitorItemPrototype prototype) {
		this.prototype = prototype;
	}
	
	public MonitorItemPrototype getMonitorItemPrototype() {
		return this.prototype;
	}
       
        public String MyItemName = null;
	
}
