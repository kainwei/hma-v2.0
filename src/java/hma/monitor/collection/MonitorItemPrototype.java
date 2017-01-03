/**
 * 
 */
package hma.monitor.collection;

/**
 * @author guoyezhi
 * 
 */
public abstract class MonitorItemPrototype {

	private String name = null;
	private String type = null;
	private long period = 180000; // 180sec by default?
	private String description = null;
	private String rawDataTypeName = null;

	public static final String MONITOR_ITEM_TYPE_BUILTIN = "built-in";
	public static final String MONITOR_ITEM_TYPE_PLUGIN = "plug-in";

	public MonitorItemPrototype(String name, String type, long period,
			String description) {
		this(name, type, period, description, "<RAWDATA> " + name);
	}
	
	public MonitorItemPrototype(String name, String type, long period,
			String description, String rawDataTypeName) {
		this.setName(name);
		this.setType(type);
		this.setPeriod(period);
		this.setDescription(description);
		this.setRawDataTypeName(rawDataTypeName);
	}
	
	/**
	 * @param name
	 *            the name to set
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

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param period
	 *            the period to set
	 */
	public void setPeriod(long period) {
		this.period = period;
	}

	/**
	 * @return the period
	 */
	public long getPeriod() {
		return period;
	}

	/**
	 * @param description
	 *            the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param rawDataTypeName the rawDataTypeName to set
	 */
	public void setRawDataTypeName(String rawDataTypeName) {
		this.rawDataTypeName = rawDataTypeName;
	}

	/**
	 * @return the rawDataTypeName
	 */
	public String getRawDataTypeName() {
		return rawDataTypeName;
	}

}