/**
 * 
 */
package hma.monitor;

import java.util.Map;

import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemPrototype;
import hma.util.TimedTask;

/**
 * @author guoyezhi
 * 
 */
public class MonitorItemCollector<RAW> extends TimedTask implements Runnable {
	
	final private MonitorItem<RAW, ?> item;
	
	final private MonitorItemPrototype prototype;

	final private Map<String, MonitorItemData<?>> rawDataPool;

	public MonitorItemCollector(
			MonitorItem<RAW,?> item,
			Map<String, MonitorItemData<?>> rawDataPool) {
		this.item = item;
		this.prototype = item.getPrototype();
		this.rawDataPool = rawDataPool;
	}

	/* 
	 * (non-Javadoc)
	 * 
	 * @see hma.util.TimedTask#run()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		
		String rawDataTypeName = prototype.getRawDataTypeName();
		
		//System.out.println("MonitorItemCollector.run() starting ... \t"
		//		+ new java.util.Date());
		
		/*
		 * MonitorItem#collect() may be a time-consuming method involving 
		 * I/O operations, so we should not put it into synchronized block!
		 */
		MonitorItemData<RAW> newRawData = item.collect();
		if (newRawData == null) {
			// TODO: add WARN logs
			return;
		}
		
		synchronized (rawDataPool) {
			MonitorItemData<RAW> oldRawData = 
				(MonitorItemData<RAW>) rawDataPool.get(rawDataTypeName);
			if (oldRawData == null ||
					(oldRawData.getTimestamp() < newRawData.getTimestamp())) {
				rawDataPool.put(rawDataTypeName, newRawData);
			} else {
				// TODO: add WARN logs
				System.out.println(
						"OldRawData.Timestamp >= NewRawData.Timestamp");
			}
		}
		
		//System.out.println("MonitorItemCollector.run() ending ... \t"
		//		+ new java.util.Date());
		
	}

}
