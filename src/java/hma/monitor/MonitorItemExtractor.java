/**
 * 
 */
package hma.monitor;

import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemPrototype;
import hma.util.TimedTask;

import java.util.Date;
import java.util.Map;

/**
 * @author guoyezhi
 *
 */
public class MonitorItemExtractor<RAW, EXTRACTED> extends TimedTask implements Runnable {
	
	final private MonitorItem<RAW, EXTRACTED> item;
	
	final private MonitorItemPrototype prototype;

	final private Map<String, MonitorItemData<?>> rawDataPool;
	
	final private Map<String, MonitorItemData<?>> extractedDataPool;
	
	public MonitorItemExtractor(
			MonitorItem<RAW, EXTRACTED> item,
			Map<String, MonitorItemData<?>> rawDataPool,
			Map<String, MonitorItemData<?>> extractedDataPool) {
		this.item = item;
		this.prototype = item.getPrototype();
		this.rawDataPool = rawDataPool;
		this.extractedDataPool = extractedDataPool;
	}
	
	/* 
	 * (non-Javadoc)
	 * 
	 * @see hma.util.TimedTask#run()
	 */
	//@SuppressWarnings("unchecked")
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		
		String monitorItemName = prototype.getName();
		String rawDataTypeName = prototype.getRawDataTypeName();
		
		//System.out.println(monitorItemName + " ---> " + System.currentTimeMillis());
		
		MonitorItemData<RAW> rawData = null;
		synchronized (rawDataPool) {
			rawData = 
				(MonitorItemData<RAW>) rawDataPool.get(rawDataTypeName);
		}
		if (rawData == null) {
			// TODO: add WARN logs
			/*
			System.out.println("RawDataPool.get(" 
					+ rawDataTypeName
					+ ") == null");
			*/
			return;
		}
		
		MonitorItemData<EXTRACTED> newExtractedData = item.extract(rawData);
		if (newExtractedData == null) {
			// TODO: add WARN logs
			System.out.println("MonitorItem#extract(rawData) == null");
			return;
		}
		newExtractedData.setName(monitorItemName);
		
		synchronized (extractedDataPool) {
			MonitorItemData<EXTRACTED> oldExtractedData = 
				(MonitorItemData<EXTRACTED>) extractedDataPool.get(monitorItemName);
			if (oldExtractedData != null) {
				long oldExtractedTime = oldExtractedData.getTimestamp();
				long newExtractedTime = newExtractedData.getTimestamp();
				if (oldExtractedTime > newExtractedTime) {
					// TODO: add WARN logs
					System.out.println(monitorItemName 
							+ " : OldExtractedData.Timestamp " 
							+ ">= NewExtractedData.Timestamp");
					return;
				} else if (oldExtractedTime == newExtractedTime) {
					// nothing needed to do
					//System.out.println(monitorItemName + " ---< " + System.currentTimeMillis());
					return;
				}
			}
			extractedDataPool.put(monitorItemName, newExtractedData);
			System.out.println("put " + monitorItemName + " # " + new Date(newExtractedData.getTimestamp()));
		}
		
		//System.out.println(monitorItemName + " ---< " + System.currentTimeMillis());
	}

}
