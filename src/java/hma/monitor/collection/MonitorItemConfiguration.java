/**
 * 
 */
package hma.monitor.collection;

import hma.conf.Configuration;
import hma.util.LOG;
import hma.util.StringHelper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * @author Vergil
 *
 */
public class MonitorItemConfiguration extends Configuration {
	
	private HashMap<String, MonitorItemPrototype> properties = null;
	
	public MonitorItemConfiguration() {
		this(true);
	}
	
	/*
	public MonitorItemConfiguration(boolean loadDefaults) {
		super(false);
		// this.setPropertyValueTagString("type");
		List<Object> resources = this.getResources();
		if (loadDefaults) {
			resources.add("D:\\eclipse\\guoyezhi\\workspace\\HMA\\conf\\monitor-items-default.xml");
			//resources.add("D:\\eclipse\\guoyezhi\\workspace\\HMA\\src\\monitor-items-site.xml");
			this.setResources(resources);
		}
		// LOG.debug(resources.toString());
	}
	*/
	
	/*
	 * TODO:
	 */
	public MonitorItemConfiguration(boolean loadDefaults) {
		super(loadDefaults, "monitor-items");
	}
	
	
	public MonitorItemConfiguration(boolean loadDefaults, String moduleName) {
		super(loadDefaults, moduleName);
	}
	
	public synchronized HashMap<String,MonitorItemPrototype> getAllPrototypes() {
		if (properties == null) {
			properties = getProps();
		}
		return new HashMap<String, MonitorItemPrototype>(properties); // return a copy
	}
	
	public synchronized HashMap<String,BuiltinMonitorItemPrototype> getBuiltinPrototypes() {
		if (properties == null) {
			properties = getProps();
		}
		HashMap<String,BuiltinMonitorItemPrototype> biProperties =
			new HashMap<String, BuiltinMonitorItemPrototype>();
		Iterator<Map.Entry<String, MonitorItemPrototype>> iter = 
			properties.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, MonitorItemPrototype> entry = iter.next();
			String name = entry.getKey();
			MonitorItemPrototype prototype = entry.getValue();
			if (prototype.getType().equals(
					MonitorItemPrototype.MONITOR_ITEM_TYPE_BUILTIN)) {
				biProperties.put(name, (BuiltinMonitorItemPrototype) prototype);
			}
		}
		return biProperties; // return a copy
	}
	
	private synchronized HashMap<String,MonitorItemPrototype> getProps() {
		
		List<Element> list = null;
		
		List<Object> resources = this.getResources();
		boolean quietmode = this.getQuietMode();
		
		if (properties == null) {
			properties = new HashMap<String,MonitorItemPrototype>();
			list = loadResources(resources, quietmode);
		
			Iterator<Element> iter = list.iterator();
			while(iter.hasNext()) {
				Element prop = iter.next();
				if (!"monitor_item".equals(prop.getTagName())) {
					// LOG.warn("bad monitor item conf file: element not <monitor_item>");
					continue;
				}
				
				NodeList fields = prop.getChildNodes();
				String name = null;
				String type = null;
				String worker = null;
			    String rawdata = null;
			    String period = null;
			    String description = null;
				// TODO! add plug-in property sub-tag
				for (int i = 0; i < fields.getLength(); i++) {
					Node fieldNode = fields.item(i);
					if (!(fieldNode instanceof Element))
						continue;
					Element field = (Element)fieldNode;
					if ("name".equals(field.getTagName()) && field.hasChildNodes())
						name = ((Text)field.getFirstChild()).getData().trim();
					if ("type".equals(field.getTagName()) && field.hasChildNodes())
						type = ((Text)field.getFirstChild()).getData().trim();
					if ("period".equals(field.getTagName()) && field.hasChildNodes())
						period = ((Text)field.getFirstChild()).getData().trim();
					if ("description".equals(field.getTagName()) && field.hasChildNodes())
						description = ((Text)field.getFirstChild()).getData().trim();
					if ("worker".equals(field.getTagName()) && field.hasChildNodes())
						worker = ((Text)field.getFirstChild()).getData().trim();
					if ("rawdata".equals(field.getTagName()) && field.hasChildNodes())
						rawdata = ((Text)field.getFirstChild()).getData().trim();
				}
				
				if (name != null && type != null && worker != null
						&& rawdata != null && period != null) {
					description = StringHelper.removeDuplicateWhitespace(description);
					LOG.info(name + "\n" + type + "\n" + worker + "\n" + rawdata + "\n"
							+ period + "\n" + description);
					MonitorItemPrototype mip = null;
					if (type.equals(MonitorItemPrototype.MONITOR_ITEM_TYPE_BUILTIN)) {
						mip = new BuiltinMonitorItemPrototype(name, type,
								Long.parseLong(period), description, worker, rawdata);
					} else if (type.equals(MonitorItemPrototype.MONITOR_ITEM_TYPE_PLUGIN)) {
						
					} else {
						throw new RuntimeException("wrong monitor item type configured");
					}
					
					properties.put(name, mip);
				}
			}
		}
		
		return properties;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MonitorItemConfiguration conf = new MonitorItemConfiguration();
		LOG.info("conf.get(\"SimulationMonitorItem\") = "
				+ conf.get("SimulationMonitorItem"));
		LOG.info("conf.get(\"SimulationMonitorItem\") = "
				+ conf.get("hadoop.job.history.cleandays"));
		//conf.getProps().list(System.out);
		LOG.info(conf.getProps().toString());
	}
	
}
