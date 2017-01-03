/**
 * 
 */
package hma.monitor;

import hma.monitor.collection.MonitorItemWorker;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemPrototype;

/**
 * @author guoyezhi
 *
 */
public class MonitorItem<RAW, EXTRACTED> {
	
	private MonitorItemWorker<RAW, EXTRACTED> worker = null;
	
	private MonitorItemPrototype prototype = null;
	
	public MonitorItem(MonitorItemWorker<RAW, EXTRACTED> worker,
			MonitorItemPrototype prototype) {
		this.worker = worker;
		this.prototype = prototype;
	}
	
	public void setPrototype(MonitorItemPrototype prototype) {
		this.prototype = prototype;
	}
	
	public MonitorItemPrototype getPrototype() {
		return prototype;
	}
	
	public MonitorItemData<RAW> collect() {
		return worker.collect();
	}

	public MonitorItemData<EXTRACTED> extract(MonitorItemData<RAW> rawData) {
		return worker.extract(rawData);
	}

}
