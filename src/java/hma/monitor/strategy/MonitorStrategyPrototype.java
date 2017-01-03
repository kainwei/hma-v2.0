/**
 * 
 */
package hma.monitor.strategy;

import hma.monitor.strategy.detect.AnomalyDetector;
import hma.monitor.strategy.detect.AnomalyDetectorGenerator;
import hma.monitor.strategy.trigger.TaskTrigger;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapterGenerater;
import hma.util.LOG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Element;

/**
 * @author guoyezhi
 *
 */
public class MonitorStrategyPrototype {
	
	private String name = null;
	
	private String alias = null;
	
	private String description = null;
	
	private long monitorPeriod = 0;
	
	private int alarmLevel =
		MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_UNINIT;
	
	private DataAcquisitionStrategy daStrategy = null;
	
	private AnomalyDetectionStrategy adStrategy = null;
	
	private TaskTriggerStrategy ttStrategy = null;
	
	public MonitorStrategyPrototype(
			String name, 
			String alais,
			String description,
			long monitorPeriod,
			int alarmLevel) {
		this.name = name;
		this.alias = alais;
		this.description = description;
		this.monitorPeriod = monitorPeriod;
		this.alarmLevel = alarmLevel;
	}
	
	public MonitorStrategyPrototype(
			String name,
			String alais,
			String description,
			String monitorPeriod,
			String alarmLevel) {
		this(name, alais, description, Long.parseLong(monitorPeriod),
				MonitorStrategyAttachedTaskAdapter.getAlarmLevelValueFromString(alarmLevel));
	}
	
	public MonitorStrategyPrototype(
			String name,
			String alais,
			String description,
			long monitorPeriod,
			int alarmLevel,
			DataAcquisitionStrategy daStrategy,
			AnomalyDetectionStrategy adStrategy,
			TaskTriggerStrategy ttStrategy) {
		this(name, alais, description, monitorPeriod, alarmLevel);
		this.setDataAcquisitionStrategy(daStrategy);
		this.setAnomalyDetectionStrategy(adStrategy);
		this.setTaskTriggerStrategy(ttStrategy);
	}
	
	public MonitorStrategyPrototype(
			String name,
			String alais,
			String description,
			String monitorPeriod,
			String alarmLevel,
			DataAcquisitionStrategy daStrategy,
			AnomalyDetectionStrategy adStrategy,
			TaskTriggerStrategy ttStrategy) {
		this(name, alais, description, Long.parseLong(monitorPeriod),
				MonitorStrategyAttachedTaskAdapter.getAlarmLevelValueFromString(alarmLevel),
				daStrategy, adStrategy, ttStrategy);
	}
	
	public class DataAcquisitionStrategy {
		
		public static final String ALL_ITEMS_CONF_CONSTANT = "#ALL ITEMS#";
		public static final String NO_ITEM_CONF_CONSTANT = "#NO ITEM#";
		
		class DataAcquisitionMetaStrategy {
			
			private String monitorItemName = null;
			private long timeout = 0; // no timeout limit by default
			private List<Element> attachedTaskConfs = null;
			
			public DataAcquisitionMetaStrategy(
					String monitorItemName,
					long timeout,
					List<Element> attachedTaskConfs) {
				this.monitorItemName = monitorItemName;
				this.timeout = timeout;
				this.attachedTaskConfs = attachedTaskConfs;
			}
		
			public String getMonitorItemName() {
				return this.monitorItemName;
			}
			
			public long getTimeout() {
				return this.timeout;
			}
			
			public List<Element> getAttachedTaskConfs() {
				return attachedTaskConfs;
			}
		}
		
		/**
		 * MonitorItemName <--> Data Acquisition Meta-Strategy
		 */
		private Map<String, DataAcquisitionMetaStrategy> metas = 
			new HashMap<String, DataAcquisitionMetaStrategy>();
		
		/**
		 * MonitorItemName <--> Attached Tasks triggered when timeout of 
		 *                      data acquisition expires.
		 */
		private Map<String, List<MonitorStrategyAttachedTaskAdapter>> 
		attachedTaskAdaptersMap = 
			new HashMap<String, List<MonitorStrategyAttachedTaskAdapter>>();
		
		
		public DataAcquisitionStrategy() {
			// nothing needed to do
		}
		
		public void putMeta(String monitorItemName, long timeout,
				List<Element> attachedTaskConfs) {
			metas.put(monitorItemName,
					new DataAcquisitionMetaStrategy(
							monitorItemName, timeout, attachedTaskConfs));
		}
		
		public void putMeta(String monitorItemName, String timeout,
				List<Element> attachedTaskConfs) {
			putMeta(monitorItemName, Long.parseLong(timeout), attachedTaskConfs);
		}
		
		public long getTimeoutMetaByMonitorItemName(String monitorItemName) {
			return metas.get(monitorItemName).getTimeout();
		}
		
		public Set<String> getAllInvolvedItemNames() {
			return metas.keySet();
		}
		
		public int getAllInvolvedItemNumber() {
			return metas.size();
		}
		
		public boolean containsMonitorItem(String monitorItemName) {
			return metas.containsKey(monitorItemName);
		}

               	public List<Element> getAttachedTaskConfsByMonitorItemName(String monitorItemName){
			return metas.get(monitorItemName).getAttachedTaskConfs();
		}
		
		private void generateAttachedTaskAdapters() {
			attachedTaskAdaptersMap =
				new HashMap<String, List<MonitorStrategyAttachedTaskAdapter>>();
			Iterator<Map.Entry<String, DataAcquisitionMetaStrategy>> iter =
				metas.entrySet().iterator();
			while (iter.hasNext()) {
				
				Map.Entry<String, DataAcquisitionMetaStrategy> entry =
					iter.next();
				String monitorItemName = entry.getKey();
				DataAcquisitionMetaStrategy meta = entry.getValue();
				
				List<MonitorStrategyAttachedTaskAdapter> adapterList =
					new ArrayList<MonitorStrategyAttachedTaskAdapter>();
				List<Element> attachedTaskConfs = meta.getAttachedTaskConfs();
				for (Element conf : attachedTaskConfs) {
					MonitorStrategyAttachedTaskAdapter adapter =
						MonitorStrategyAttachedTaskAdapterGenerater
						.generateMonitorStrategyAttachedTaskAdapter(conf);
					
					if (adapter == null) {
						LOG.warn("Can't generate attached task adapter from conf: "
								+ conf);
						continue;
					}
					
					adapterList.add(adapter);
				}
				
				/*
				 * 'adapterList' will have no elements if there are no attached
				 * tasks configured for relevant acquired monitor item.
				 */
				attachedTaskAdaptersMap.put(monitorItemName, adapterList);
			}
			
		}
		
		public List<MonitorStrategyAttachedTaskAdapter>
		getAttachedTaskAdaptersByMonitorItemName(
				String monitorItemName) {
			if (attachedTaskAdaptersMap.size() == 0) {
				generateAttachedTaskAdapters();
			}
			return attachedTaskAdaptersMap.get(monitorItemName);
		}
	}
	
	
	class AnomalyDetectionStrategy {
		
		class AnomalyDetectionMetaStrategy {
			
			private String monitorItemName = null;
			private String monitorDataTypeName = null;
			private String monitorDataSubTypeName = null;
			private Element strategy = null;
			
			public AnomalyDetectionMetaStrategy(
					String monitorItemName,
					String monitorDataTypeName,
					String monitorDataSubTypeName,
					Element strategy) {
				this.monitorItemName = monitorItemName;
				this.monitorDataTypeName = monitorDataTypeName;
				this.monitorDataSubTypeName = monitorDataSubTypeName;
				this.strategy = strategy;
			}
			
			/**
			 * @return the monitorItemName
			 */
			public String getMonitorItemName() {
				return monitorItemName;
			}
			
			/**
			 * @return the monitorDataTypeName
			 */
			public String getMonitorDataTypeName() {
				return monitorDataTypeName;
			}
			
			/**
			 * @return the monitorDataSubTypeName
			 */
			public String getMonitorDataSubTypeName() {
				return monitorDataSubTypeName;
			}
			
			/**
			 * @return the strategy
			 */
			public Element getStrategy() {
				return strategy;
			}
			
		}
		
		private Map<String, AnomalyDetectionMetaStrategy> metas = 
			new HashMap<String, AnomalyDetectionMetaStrategy>();
		
		private List<AnomalyDetector> detectors = 
			new ArrayList<AnomalyDetector>();
		
		public AnomalyDetectionStrategy() {
			// nothing needed to do
		}
		
		public void putMeta(String monitorItemName,
				String monitorDataTypeName,
				String monitorDataSubTypeName,
				Element strategy) {
			metas.put(monitorItemName, 
					new AnomalyDetectionMetaStrategy(
							monitorItemName, monitorDataTypeName,
							monitorDataSubTypeName, strategy));
		}
		
		/**
		 * This method should be called whenever all involved <b>meta</b>s
		 * have been inserted.
		 */
		private void generateAnomalyDetectors() {
			detectors = new ArrayList<AnomalyDetector>();
			Iterator<Map.Entry<String, AnomalyDetectionMetaStrategy>> iter =
				metas.entrySet().iterator();
			while (iter.hasNext()) {
				AnomalyDetectionMetaStrategy meta = iter.next().getValue();
				AnomalyDetector detector = 
					AnomalyDetectorGenerator.generate(
							meta.getMonitorItemName(),
							meta.getMonitorDataTypeName(),
							meta.getMonitorDataSubTypeName(),
							meta.getStrategy());
				if (detector == null) {
					LOG.warn("Can't generate anomaly detector from conf: "
							+ meta);
					continue;
				}
				detectors.add(detector);
			}
		}
		
		public List<AnomalyDetector> getAllInvolvedAnomalyDetectors() {
			if (detectors.size() == 0) {
				generateAnomalyDetectors();
			}
			return detectors;
		}
	}
	
	
	class TaskTriggerStrategy {
		
		private String mode = null;
		private Element strategy = null;
		private List<Element> attachedTaskConfs = null;
		
		private TaskTrigger visitor = null;
		
		private List<MonitorStrategyAttachedTaskAdapter>
		attachedTaskAdapters = 
			new ArrayList<MonitorStrategyAttachedTaskAdapter>();
		
		public TaskTriggerStrategy(String mode, Element strategy,
				List<Element> attachedTaskConfs) {
			this.mode = mode;
			this.strategy = strategy;
			this.attachedTaskConfs = attachedTaskConfs;
			
			this.generateTaskTrigger();
			this.generateAttachedTaskAdapters();
		}
		
		private void generateTaskTrigger() {
			try {
				visitor = (TaskTrigger) Class.forName(
						"hma.monitor.strategy.trigger." 
						+ mode
						+ "TypeTaskTrigger").getConstructor(
								Element.class).newInstance(strategy);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		
		public TaskTrigger getTaskTrigger() {
			return this.visitor;
		}
		
		private void generateAttachedTaskAdapters() {
			
			attachedTaskAdapters =
				new ArrayList<MonitorStrategyAttachedTaskAdapter>();
			
			for (Element conf : attachedTaskConfs) {
				MonitorStrategyAttachedTaskAdapter adapter = 
					MonitorStrategyAttachedTaskAdapterGenerater
					.generateMonitorStrategyAttachedTaskAdapter(conf);
				if (adapter == null) {
					LOG.warn("Can't generate attached task adapter from conf: "
							+ conf);
					continue;
				}
				attachedTaskAdapters.add(adapter);
			}
			
		}
		
		public List<MonitorStrategyAttachedTaskAdapter> getAttachedTaskAdapters() {
			return this.attachedTaskAdapters;
		}
		
	}
	
	
	
	/**
	 * @param daStrategy the daStrategy to set
	 */
	public void setDataAcquisitionStrategy(DataAcquisitionStrategy daStrategy) {
		this.daStrategy = daStrategy;
	}

	/**
	 * @return the daStrategy
	 */
	public DataAcquisitionStrategy getDataAcquisitionStrategy() {
		return daStrategy;
	}

	/**
	 * @param adStrategy the adStrategy to set
	 */
	public void setAnomalyDetectionStrategy(AnomalyDetectionStrategy adStrategy) {
		this.adStrategy = adStrategy;
	}

	/**
	 * @return the adStrategy
	 */
	public AnomalyDetectionStrategy getAnomalyDetectionStrategy() {
		return adStrategy;
	}

	/**
	 * @param ttStrategy the ttStrategy to set
	 */
	public void setTaskTriggerStrategy(TaskTriggerStrategy ttStrategy) {
		this.ttStrategy = ttStrategy;
	}

	/**
	 * @return the ttStrategy
	 */
	public TaskTriggerStrategy getTaskTriggerStrategy() {
		return ttStrategy;
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

	/**
	 * @param alias the alias to set
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * @return the alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param description the description to set
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
	 * @param monitorPeriod the monitorPeriod to set
	 */
	public void setMonitorPeriod(long monitorPeriod) {
		this.monitorPeriod = monitorPeriod;
	}

	/**
	 * @return the monitorPeriod
	 */
	public long getMonitorPeriod() {
		return monitorPeriod;
	}

	/**
	 * @param alarmLevel the alarmLevel to set
	 */
	public void setAlarmLevel(int alarmLevel) {
		this.alarmLevel = alarmLevel;
	}

	/**
	 * @return the alarmLevel
	 */
	public int getAlarmLevel() {
		return alarmLevel;
	}

}
