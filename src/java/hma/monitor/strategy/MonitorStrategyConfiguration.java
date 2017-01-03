/**
 * 
 */
package hma.monitor.strategy;

import hma.conf.Configuration;
import hma.monitor.strategy.MonitorStrategyPrototype.AnomalyDetectionStrategy;
import hma.monitor.strategy.MonitorStrategyPrototype.DataAcquisitionStrategy;
import hma.monitor.strategy.MonitorStrategyPrototype.TaskTriggerStrategy;
import hma.util.LOG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * @author guoyezhi
 *
 */
public class MonitorStrategyConfiguration extends Configuration {
	
	private HashMap<String, MonitorStrategyPrototype> prototypes = null;
	
	public MonitorStrategyConfiguration() {
		this(true);
	}
	
	/*
	public MonitorStrategyConfiguration(boolean loadDefaults) {
		super(false);
		List<Object> resources = this.getResources();
		if (loadDefaults) {
			resources.add("D:\\eclipse\\guoyezhi\\workspace\\HMA\\conf\\monitor-strategies-default.xml");
			//resources.add("D:\\eclipse\\guoyezhi\\workspace\\HMA\\src\\monitor-items-site.xml");
			this.setResources(resources);
		}
		// LOG.debug(resources.toString());
	}
	*/
	
	/*
	 * TODO:
	 */
	public MonitorStrategyConfiguration(boolean loadDefaults) {
		super(loadDefaults, "monitor-strategies");
	}
	
	
	public synchronized HashMap<String, MonitorStrategyPrototype> getAllPrototypes() {
		if (prototypes == null) {
			prototypes = getPrototypes();
		}
		return new HashMap<String, MonitorStrategyPrototype>(prototypes); // return a copy
	}
	
	private synchronized HashMap<String, MonitorStrategyPrototype> getPrototypes() {
		
		if (prototypes == null) {
			
			prototypes = new HashMap<String, MonitorStrategyPrototype>();
			
			List<Object> resources = this.getResources();
			boolean quietmode = this.getQuietMode();
			List<Element> list = loadResources(resources, quietmode);
			
			Iterator<Element> iter = list.iterator();
			while(iter.hasNext()) {
				
				Element elem = iter.next();
				if (!"monitor_strategy".equals(elem.getTagName())) {
					LOG.warn("bad monitor strategy conf file: element not <monitor_strategy>");
					continue;
				}
				
				this.generatePrototype(elem, prototypes);
			}
		}
		
		return prototypes;
	}
	
	
	private static final String DEFAULT_MONITOR_PERIOD_CONF = "60000";
	
	private static final String DEFAULT_ALARM_LEVEL_CONF = "WARNING";
	
	/**
	 * Internal core implementation of configuration parsing 
	 * & prototype generation
	 */
	private synchronized void generatePrototype(Element entryConf,
			HashMap<String, MonitorStrategyPrototype> prototypes) {
		
		NodeList propNodes = entryConf.getChildNodes();
		
		String nameConf = null;
		String aliasConf = null;
		String descriptionConf = null;
		String periodConf = DEFAULT_MONITOR_PERIOD_CONF; // monitor_period
		String levelConf = DEFAULT_ALARM_LEVEL_CONF; // alarm_level
	    MonitorStrategyPrototype.DataAcquisitionStrategy daStrategy = null;
	    MonitorStrategyPrototype.AnomalyDetectionStrategy adStrategy = null;
	    MonitorStrategyPrototype.TaskTriggerStrategy ttStrategy = null;
	    
	    Element daConf = null;
	    Element adConf = null;
	    Element ttConf = null;
	    
		for (int i = 0; i < propNodes.getLength(); i++) {
			
			Node propNode = propNodes.item(i);
			if (!(propNode instanceof Element))
				continue;
			
			Element prop = (Element)propNode;
			if ("strategy_name".equals(prop.getTagName()) && prop.hasChildNodes())
				nameConf = ((Text)prop.getFirstChild()).getData().trim();
			if ("strategy_alias".equals(prop.getTagName()) && prop.hasChildNodes())
				aliasConf = ((Text)prop.getFirstChild()).getData().trim();
			if ("strategy_description".equals(prop.getTagName()) && prop.hasChildNodes())
				descriptionConf = ((Text)prop.getFirstChild()).getData().trim();
			if ("monitor_period".equals(prop.getTagName()) && prop.hasChildNodes())
				periodConf = ((Text)prop.getFirstChild()).getData().trim();
			if ("alarm_level".equals(prop.getTagName()) && prop.hasChildNodes())
				levelConf = ((Text)prop.getFirstChild()).getData().trim();
			if ("data_acquisition".equals(prop.getTagName()) && prop.hasChildNodes())
				daConf = prop;
			if ("anomaly_detection".equals(prop.getTagName()) && prop.hasChildNodes())
				adConf = prop;
			if ("task_trigger".equals(prop.getTagName()) && prop.hasChildNodes())
				ttConf = prop;
		}
		
		if (nameConf == null) {
			if (this.getQuietMode() == false) {
				LOG.warn("Syntax errors in entry conf: No strategy name configured.");
			}
			return;
		}
		
		MonitorStrategyPrototype msp = new MonitorStrategyPrototype(
				nameConf, aliasConf, descriptionConf, periodConf, levelConf);
		
		daStrategy = this.parseDataAcquisitionConf(daConf, msp);
		adStrategy = this.parseAnomalyDetectionConf(adConf, msp);
		ttStrategy = this.parseTaskTriggerConf(ttConf, msp);
		
		if (daStrategy != null && adStrategy != null && ttStrategy != null) {
			msp.setDataAcquisitionStrategy(daStrategy);
			msp.setAnomalyDetectionStrategy(adStrategy);
			msp.setTaskTriggerStrategy(ttStrategy);
			prototypes.put(nameConf, msp);
			return;
		}
		
		if (this.getQuietMode() == false) {
			LOG.warn("Syntax errors in entry conf: " + nameConf);
			LOG.warn("daStrategy = " + daStrategy);
			LOG.warn("adStrategy = " + adStrategy);
			LOG.warn("ttStrategy = " + ttStrategy);
		}
	}
	
	private DataAcquisitionStrategy parseDataAcquisitionConf(
			Element e, MonitorStrategyPrototype prototype) {
		
		LOG.warn("e = " + e);
		
		DataAcquisitionStrategy daStrategy = 
			prototype.new DataAcquisitionStrategy();
		
		NodeList itemNodes = e.getChildNodes();
		for (int i = 0; i < itemNodes.getLength(); i++) {
			
			Node itemNode = itemNodes.item(i);
			if (!(itemNode instanceof Element))
				continue;
			Element item = (Element) itemNode;
			if ("item".equals(item.getTagName()) == false) {
				continue;
			}
			
			NodeList fieldNodes = item.getChildNodes();
			String itemName = null;
			String timeout = null;
			List<Element> attachedTaskConfs = new ArrayList<Element>();
			for (int j = 0; j < fieldNodes.getLength(); j++) {
				Node fieldNode = fieldNodes.item(j);
				if (!(fieldNode instanceof Element))
					continue;
				Element field = (Element)fieldNode;
				if ("name".equals(field.getTagName()) && field.hasChildNodes())
					itemName = ((Text)field.getFirstChild()).getData().trim();
				if ("timeout".equals(field.getTagName()) && field.hasChildNodes())
					timeout = ((Text)field.getFirstChild()).getData().trim();
				if ("attached_tasks".equals(field.getTagName()) && field.hasChildNodes()) {
					NodeList subFieldNodes = field.getChildNodes();
					for (int k = 0; k < subFieldNodes.getLength(); k++) {
						Node subFieldNode = subFieldNodes.item(k);
						if (!(subFieldNode instanceof Element))
							continue;
						Element subField = (Element) subFieldNode;
						if ("task".equals(subField.getTagName())
								&& subField.hasChildNodes())
							attachedTaskConfs.add(subField);
					}
				}
			}
			
			if (itemName != null && timeout != null) {
				daStrategy.putMeta(itemName, timeout, attachedTaskConfs);
			}
			
			LOG.warn("itemName = " + itemName);
			LOG.warn("timeout = " + timeout);
		}
		
		if (daStrategy.getAllInvolvedItemNumber() > 0) {
			return daStrategy;
		}
		
		return null;
	}
	
	private AnomalyDetectionStrategy parseAnomalyDetectionConf(
			Element e, MonitorStrategyPrototype prototype) {
		
		LOG.warn("e = " + e);
		
		AnomalyDetectionStrategy adStrategy =
			prototype.new AnomalyDetectionStrategy();
		
		NodeList itemNodes = e.getChildNodes();
		for (int i = 0; i < itemNodes.getLength(); i++) {
			
			Node itemNode = itemNodes.item(i);
			if (!(itemNode instanceof Element))
				continue;
			Element item = (Element)itemNode;
			if ("item".equals(item.getTagName()) == false) {
				continue;
			}
			
			NodeList fieldNodes = item.getChildNodes();
			
			String monitorItemName = null;
			String monitorDataType = null;
			String monitorDataSubType = null;
			Element strategy = null;
			
			for (int j = 0; j < fieldNodes.getLength(); j++) {
				Node fieldNode = fieldNodes.item(j);
				if (!(fieldNode instanceof Element))
					continue;
				Element field = (Element)fieldNode;
				if ("name".equals(field.getTagName()) && field.hasChildNodes())
					monitorItemName = ((Text)field.getFirstChild()).getData().trim();
				if ("type".equals(field.getTagName()) && field.hasChildNodes())
					monitorDataType = ((Text)field.getFirstChild()).getData().trim();
				if ("sub-type".equals(field.getTagName()) && field.hasChildNodes())
					monitorDataSubType = ((Text)field.getFirstChild()).getData().trim();
				if ("strategy".equals(field.getTagName()) && field.hasChildNodes())
					strategy = field;
			}
			
			if (monitorItemName != null && monitorDataType != null
					&& strategy != null) {
				adStrategy.putMeta(monitorItemName,
						monitorDataType, monitorDataSubType, strategy);
			}
			
			//LOG.warn("monitorItemName = " + monitorItemName);
			//LOG.warn("monitorDataType = " + monitorDataType);
			//LOG.warn("monitorDataSubType = " + monitorDataSubType);
		}
		
		if (adStrategy.getAllInvolvedAnomalyDetectors().size() > 0) {
			return adStrategy;
		}
		
		return null;
	}
	
    private TaskTriggerStrategy parseTaskTriggerConf(Element e,
    		MonitorStrategyPrototype prototype) {
    	
    	NodeList fieldNodes = e.getChildNodes();
    	
    	String mode = null;
    	Element strategy = null;
    	List<Element> attachedTaskConfs = new ArrayList<Element>();
    	
    	for (int i = 0; i < fieldNodes.getLength(); i++) {
			Node fieldNode = fieldNodes.item(i);
			if (!(fieldNode instanceof Element))
				continue;
			Element field = (Element)fieldNode;
			if ("mode".equals(field.getTagName()) && field.hasChildNodes())
				mode = ((Text)field.getFirstChild()).getData().trim();
			if ("strategy".equals(field.getTagName()) && field.hasChildNodes())
				strategy = field;
			if ("attached_tasks".equals(field.getTagName()) && field.hasChildNodes()) {
				NodeList subFieldNodes = field.getChildNodes();
				for (int k = 0; k < subFieldNodes.getLength(); k++) {
					Node subFieldNode = subFieldNodes.item(k);
					if (!(subFieldNode instanceof Element))
						continue;
					Element subField = (Element) subFieldNode;
					if ("task".equals(subField.getTagName())
							&& subField.hasChildNodes())
						attachedTaskConfs.add(subField);
				}
			}
		}
		
		if (mode != null && (attachedTaskConfs.size() > 0)) {
			return prototype.new TaskTriggerStrategy(mode, strategy, attachedTaskConfs);
		}
    	
    	return null;
    }
    
}
