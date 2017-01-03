/**
 * 
 */
package hma.monitor.strategy;

import hma.monitor.collection.MonitorItemData;

/**
 * @author guoyezhi
 * add additionalInfo by chiwen01
 *
 */
public class MonitorStrategyData<TYPE> {
	
	private String itemName = null;
	
	private TYPE data = null;
	
	private long timestamp = 0;

	private String additionalInfo = null;
	
	public MonitorStrategyData(MonitorItemData<TYPE> itemData) {
		this.itemName = itemData.getName();
		this.data = itemData.getData();
		this.timestamp = itemData.getTimestamp();
		this.additionalInfo = itemData.getAdditionalInfo();
	}
	
	public MonitorStrategyData(String itemName, TYPE data, long timestamp){
		this.itemName = itemName;
		this.data = data;
		this.timestamp = timestamp;
	}

	public MonitorStrategyData(String itemName, TYPE data, long timestamp, String additionalInfo){
		this.itemName = itemName;
		this.data = data;
		this.timestamp = timestamp;
		this.additionalInfo = additionalInfo;
	}
	
	/**
	 * @param itemName the itemName to set
	 */
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	
	/**
	 * @return the itemName
	 */
	public String getItemName() {
		return itemName;
	}
	
	/**
	 * @param data the data to set
	 */
	public void setData(TYPE data) {
		this.data = data;
	}
	
	/**
	 * @return the data
	 */
	public TYPE getData() {
		return data;
	}
	
	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}
	
	/**
	 * @param additionalInfo the additionalInfo to set
	 */
	public void setAdditionalInfo(String additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
	
	/**
	 * @return the additionalInfo
	 */
	public String getAdditionalInfo() {
		return additionalInfo;
	}
	
}
