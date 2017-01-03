/**
 * 
 */
package hma.monitor.collection;

/**
 * This class represents a monitor item data (both raw type and extracted type).
 * The difference between a monitor item data's raw-type MonitorItemData object 
 * and its extracted-type MonitorItemData object is that <b>data</b> properties 
 * of these two objects are probably different.  That is, <b>name</b> and 
 * <b>timestamp</b> properties of these two objects should be consistent.
 * 
 * @author guoyezhi
 * add additionalInfo by chiwen01
 * 
 */
public class MonitorItemData<TYPE> {

	private String name;

	private TYPE data = null;

	private long timestamp = 0;
    
    private String additionalInfo = null;

	public MonitorItemData(String name, TYPE data, long timestamp) {
		this.setName(name);
		this.data = data;
		this.timestamp = timestamp;
	}

	public MonitorItemData(String name, TYPE data, long timestamp, String additionalInfo) {
		this.setName(name);
		this.data = data;
		this.timestamp = timestamp;
        this.additionalInfo = additionalInfo;
	}
	
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	public TYPE getData() {
		return data;
	}

	public void setData(TYPE data) {
		this.data = data;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getAdditionalInfo() {
		return additionalInfo;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public void setAdditionalInfo(String additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
    
}
