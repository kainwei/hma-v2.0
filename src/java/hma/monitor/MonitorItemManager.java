/**
 * 
 */
package hma.monitor;

import hma.conf.Configured;
import hma.monitor.collection.BuiltinMonitorItemPrototype;
import hma.monitor.collection.MonitorItemConfiguration;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemPrototype;
import hma.monitor.collection.MonitorItemWorker;
import hma.monitor.collection.metric.CollectionMetricDBDumper;
import hma.util.BuiltinPrototypesInjector;
import hma.util.TimedTaskManager;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * @author guoyezhi
 * add additional by chiwen01
 * 
 */
public class MonitorItemManager extends Configured implements Runnable {

	/** 
	 * Raw Data Pool of Monitored Items
	 */
	private Map<String, MonitorItemData<?>> rawDataPool = 
		new HashMap<String, MonitorItemData<?>>();
	
	/** 
	 * Target Data Pool of Monitored Items extracted from Raw Data Pool
	 */
	private Map<String, MonitorItemData<?>> extractedDataPool = 
		new HashMap<String, MonitorItemData<?>>();
	
	private Map<String, BuiltinMonitorItemPrototype> allBuiltinPrototypes = null;
	@SuppressWarnings("unused")
	private Map<String, BuiltinMonitorItemPrototype> uniqueBuiltinPrototypesByRawData = null;
	
	private Map<String, MonitorItemWorker<?,?>> allBuiltinWorkers = null;
	// TODO: private Map<String, MonitorItemWorker<?,?>> allPluginWorkers = null;
	
	private Map<String, MonitorItem<?,?>> allMonitorItems = null;
	
	private Map<String, MonitorItemCollector<?>> allCollectors = null;
	private Map<String, MonitorItemExtractor<?,?>> allExtractors = null;
	
	private TimedTaskManager builtinCollectorTimedScheduler = null;
	// TODO: private TimedTaskManager pluginCollectorTimedScheduler = null;
	private TimedTaskManager extractorTimedScheduler = null;
	
	
	final private MonitorManager monitorManager;
	
	private static int monitorItemWorkerNum = 0;
	
	private static int extractionPeriod = 10;
	
	
	private Random rand = null;
	
	
	public MonitorItemManager() {
		super(new MonitorItemConfiguration(true));
		monitorManager = MonitorManager.getMonitorManager();
		monitorItemWorkerNum = 
			monitorManager.getConf().getInt(
					"monitor.item.worker.num", 1024);
		extractionPeriod =
			monitorManager.getConf().getInt(
					"monitor.item.data.extraction.period", 1);
		builtinCollectorTimedScheduler = 
			new TimedTaskManager(monitorItemWorkerNum);
		extractorTimedScheduler = 
			new TimedTaskManager(monitorItemWorkerNum);
		rand = new Random();
	}
	
	
	private synchronized void initBuiltinMonitorItemWorkers() {
		
		MonitorItemConfiguration miConf =
			(MonitorItemConfiguration) this.getConf();
		
		if (allBuiltinPrototypes == null) {
			allBuiltinPrototypes = miConf.getBuiltinPrototypes();
			BuiltinPrototypesInjector.BuiltinPrototypesInjector(allBuiltinPrototypes);
			
		}
		
		if (allBuiltinWorkers == null) {
			allBuiltinWorkers = new HashMap<String,MonitorItemWorker<?,?>>();
		}
		
		Iterator<Map.Entry<String, BuiltinMonitorItemPrototype>> iter =
			allBuiltinPrototypes.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, BuiltinMonitorItemPrototype> entry = iter.next();
			String name = entry.getKey();
                        //System.out.println("kain's log about BuiltinItem : " + name);
			BuiltinMonitorItemPrototype prototype = entry.getValue();
			MonitorItemWorker<?,?> worker = null;
			try {
				worker = (MonitorItemWorker<?,?>) 
					prototype.getMonitorItemWorkerClass().newInstance();
				worker.setMonitorItemPrototype(prototype);
			        worker.MyItemName = name;
			
			} catch (InstantiationException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			allBuiltinWorkers.put(name, worker);
		}
		
	}
	
	@SuppressWarnings("unused")
	private synchronized void initPluginMonitorItemWorkers() {
		// TODO
	}
	
	@SuppressWarnings("unchecked")
	private synchronized void initAllMonitorItems() {
		if (this.allBuiltinWorkers == null) {
			this.initBuiltinMonitorItemWorkers();
		}
		// TODO: if (this.allPluginWorkers == null) { }
		
		if (this.allMonitorItems == null) {
			this.allMonitorItems = new HashMap<String, MonitorItem<?,?>>();
		}
		
		Iterator<Map.Entry<String, BuiltinMonitorItemPrototype>> iter =
			this.allBuiltinPrototypes.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, BuiltinMonitorItemPrototype> entry = iter.next();
			String name = entry.getKey();
			MonitorItemPrototype prototype = entry.getValue();
			MonitorItemWorker<?,?> worker = this.allBuiltinWorkers.get(name);
			if (prototype == null || worker == null) {
				throw new RuntimeException(
						"Prototype & Worker is not paired with the other.");
			}
			MonitorItem<?,?> item =
				(MonitorItem<?,?>) new MonitorItem(worker, prototype);
			this.allMonitorItems.put(name, item);
		}
	}
	
	@SuppressWarnings("unchecked")
	private synchronized void initAllCollectors() {
		if (this.allMonitorItems == null) {
			this.initAllMonitorItems();
		}
		if (this.allCollectors == null) {
			this.allCollectors = new HashMap<String, MonitorItemCollector<?>>();
		}
		Iterator<Map.Entry<String, MonitorItem<?,?>>> iter =
			this.allMonitorItems.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, MonitorItem<?,?>> entry = iter.next();
			String name = entry.getKey();
			MonitorItem<?,?> item = entry.getValue();
			MonitorItemCollector<?> collector = 
				new MonitorItemCollector(item, rawDataPool);
			this.allCollectors.put(name, collector);
		}
	}
	
	@SuppressWarnings("unchecked")
	private synchronized void initAllExtractors() {
		if (this.allMonitorItems == null) {
			this.initAllMonitorItems();
		}
		if (this.allExtractors == null) {
			this.allExtractors = new HashMap<String, MonitorItemExtractor<?,?>>();
		}
		Iterator<Map.Entry<String, MonitorItem<?,?>>> iter =
			this.allMonitorItems.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, MonitorItem<?,?>> entry = iter.next();
			String name = entry.getKey();
			MonitorItem<?,?> item = entry.getValue();
			MonitorItemExtractor<?,?> extractor =
				new MonitorItemExtractor(item, rawDataPool, getExtractedDataPool());
			this.allExtractors.put(name, extractor);
		}
	}
	
	private synchronized void launchBuiltinCollectors								() {
		
		Map<String, BuiltinMonitorItemPrototype> uniqueBuiltinPrototypes =
			new HashMap<String, BuiltinMonitorItemPrototype>();
		
		Iterator<Map.Entry<String, BuiltinMonitorItemPrototype>> iter =
			this.allBuiltinPrototypes.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, BuiltinMonitorItemPrototype> entry = iter.next();
			BuiltinMonitorItemPrototype prototype = entry.getValue();
			String rawDataTypeName = prototype.getRawDataTypeName();
			String name = prototype.getName();
                        System.out.println("kain's log about builtin prototyp name : " + name );
//			BuiltinMonitorItemPrototype tmp = 
//				uniqueBuiltinPrototypes.get(rawDataTypeName);
			BuiltinMonitorItemPrototype tmp = 
					uniqueBuiltinPrototypes.get(name);
			if (tmp == null || tmp.getPeriod() > prototype.getPeriod()) {
				//uniqueBuiltinPrototypes.put(rawDataTypeName, prototype);
				uniqueBuiltinPrototypes.put(name, prototype);
			}
		}
		
		Iterator<Map.Entry<String, BuiltinMonitorItemPrototype>> iter2 =
			uniqueBuiltinPrototypes.entrySet().iterator();
		while (iter2.hasNext()) {
			Map.Entry<String, BuiltinMonitorItemPrototype> entry = iter2.next();
			String iterName = entry.getValue().getName();
			MonitorItemCollector<?> collector = this.allCollectors.get(iterName);
			this.builtinCollectorTimedScheduler.scheduleAtFixedRate(
					collector, rand.nextInt(60000), entry.getValue().getPeriod());
		}
		
		this.uniqueBuiltinPrototypesByRawData = uniqueBuiltinPrototypes;
	}
	
	private void launchPluginCollectors() {
		// TODO
	}
	
	
	private void launchMonitorItemCollectors() {
		this.initAllCollectors();
		this.launchBuiltinCollectors();
		this.launchPluginCollectors();
	}
	
	
	
	private void launchMonitorItemExtractors() {
		this.initAllExtractors();
		Iterator<Map.Entry<String, MonitorItemExtractor<?,?>>> iter =
			this.allExtractors.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, MonitorItemExtractor<?, ?>> entry = iter.next();
			MonitorItemExtractor<?, ?> extractor = entry.getValue();
			this.extractorTimedScheduler.scheduleAtFixedRate(
					extractor, 0, (extractionPeriod * 1000));
		}
	}
	
	
	@Override
	public void run() {
		boolean metricCollected = 
			MonitorManager.getGlobalConf().getBoolean(
					"monitor.collection.metric.enable", true);
		if (metricCollected) {
			new Thread(new CollectionMetricDBDumper()).start();
			try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.launchMonitorItemCollectors();
		this.launchMonitorItemExtractors();
	}
	
	/**
	 * @param extractedDataPool the extractedDataPool to set
	 */
	public void setExtractedDataPool(Map<String, MonitorItemData<?>> extractedDataPool) {
		this.extractedDataPool = extractedDataPool;
	}
	
	/**
	 * @return the extractedDataPool
	 */
	public Map<String, MonitorItemData<?>> getExtractedDataPool() {
		return extractedDataPool;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		MonitorItemManager mim = new MonitorItemManager();
		mim.run();
		
		for (int i = 0; i < 30; i++) {
			
			synchronized (mim.rawDataPool) {
				System.out.println("--- RAW DATA POOL (" 
						+ mim.rawDataPool.size() +") --- ");
				Iterator<Map.Entry<String, MonitorItemData<?>>> iter = 
					mim.rawDataPool.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry<String, MonitorItemData<?>> entry = iter.next();
					//String itemName = entry.getKey();
					MonitorItemData<?> itemData = entry.getValue();
					System.out.println("\t---> "
							+ itemData.getName() + "\t"
							+ itemData.getData() + "\t"
							+ new Date(itemData.getTimestamp()) + "\t"
							+ itemData.getAdditionalInfo());
				}
			}
			
			synchronized (mim.extractedDataPool) {
				System.out.println("--- EXT DATA POOL (" 
						+ mim.getExtractedDataPool().size() +") --- ");
				Iterator<Map.Entry<String, MonitorItemData<?>>> iter = 
					mim.getExtractedDataPool().entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry<String, MonitorItemData<?>> entry = iter.next();
					//String itemName = entry.getKey();
					MonitorItemData<?> itemData = entry.getValue();
					System.out.println("\t---> "
							+ itemData.getName() + "\t"
							+ itemData.getData() + "\t"
							+ new Date(itemData.getTimestamp()) + "\t"
							+ itemData.getAdditionalInfo());
				}
			}
			
			try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/*
			System.out.println("--- RAW DATA POOL (" + mim.rawDataPool.size() +") ---> " + 
					new Date(mim.rawDataPool.get("hma.monitor.collection.SimMonitorItemRawData").getTimestamp()));
			System.out.println("--- EXT DATA POOL (" + mim.getExtractedDataPool().size() +") ---> "
					+ mim.getExtractedDataPool().get("SimulationMonitorItem").getName() + " "
					+ mim.getExtractedDataPool().get("SimulationMonitorItem").getData());
					// + new Date(mim.extractedDataPool.get("SimulationMonitorItem").getTimestamp()));
			System.out.println("--- EXT DATA POOL (" + mim.getExtractedDataPool().size() +") ---> "
					+ mim.getExtractedDataPool().get("SimulationMonitorItem2").getName() + " "
					+ mim.getExtractedDataPool().get("SimulationMonitorItem2").getData());
			System.out.println("--- EXT DATA POOL (" + mim.getExtractedDataPool().size() +") ---> "
					+ mim.getExtractedDataPool().get("SimulationMonitorItem3").getName() + " "
					+ mim.getExtractedDataPool().get("SimulationMonitorItem3").getData());
			try {
				TimeUnit.SECONDS.sleep(7);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("--- EXT DATA POOL (" + mim.getExtractedDataPool().size() +") ---> "
					+ mim.getExtractedDataPool().get("NameNode_ListHDFSRootPath").getName() + " "
					+ mim.getExtractedDataPool().get("NameNode_ListHDFSRootPath").getData());
			*/
		}
	}

}
