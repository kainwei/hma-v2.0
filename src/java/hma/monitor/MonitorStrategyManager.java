/**
 * 
 */
package hma.monitor;

import hma.conf.Configured;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.strategy.MonitorStrategyConfiguration;
import hma.monitor.strategy.MonitorStrategyPrototype;
import hma.monitor.strategy.MonitorStrategyWorker;
import hma.util.TimedTaskManager;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import java.util.Date;
import hma.util.DaStrategyInjector;

/**
 * @author guoyezhi
 *
 */
public class MonitorStrategyManager extends Configured implements Runnable {
	
	private Map<String, MonitorItemData<?>> extractedDataPool = null;
	
	private Map<String, MonitorStrategyPrototype> allStrategyPrototypes = null;
	
	private Map<String, MonitorStrategy> allStrategies = null;
	
	private Map<String, MonitorStrategyWorker> allStrategyWorkers = null;
	
	private TimedTaskManager strategyWorkerTimedScheduler = null;
	
	private static final int MAX_MONITOR_STRATEGY_WORKERS = 40;
	
	private Random rand = null;
	
	
	/**
	 * 
	 * @param extractedDataPool
	 */
	public MonitorStrategyManager(Map<String, MonitorItemData<?>> extractedDataPool) {
		super(new MonitorStrategyConfiguration(true));
		this.extractedDataPool = extractedDataPool;
		this.strategyWorkerTimedScheduler =
			new TimedTaskManager(MAX_MONITOR_STRATEGY_WORKERS);
		this.rand = new Random();
	}
	
	private synchronized void initStrategyPrototypes() {
		if (allStrategyPrototypes == null) {
			MonitorStrategyConfiguration msConf =
				(MonitorStrategyConfiguration) this.getConf();
			allStrategyPrototypes = msConf.getAllPrototypes();
                        DaStrategyInjector.DaStrategyInjector(allStrategyPrototypes);
		}
		//System.out.println("!!!---> " + allStrategyPrototypes.size()
		//		+ "Strategy Prototype(s) <---!!!");
	}
	
	private synchronized void initAllStrategies() {
		if (allStrategies == null) {
			
			if (allStrategyPrototypes == null) {
				initStrategyPrototypes();
				if (allStrategyPrototypes == null) {
					throw new RuntimeException(
							"Invaild Monitor Strategy Conf");
				}
			}
			
			allStrategies = Collections.synchronizedMap(
					new HashMap<String, MonitorStrategy>());
			Iterator<Map.Entry<String, MonitorStrategyPrototype>> iter =
				this.allStrategyPrototypes.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, MonitorStrategyPrototype> entry =
					iter.next();
				String strategyName = entry.getKey();
				MonitorStrategyPrototype prototype = entry.getValue();
				MonitorStrategy strategy = new MonitorStrategy(
						prototype, this.extractedDataPool);
				this.allStrategies.put(strategyName, strategy);
			}
			if (allStrategies.size() == 0) {
				allStrategies = null;
			}
		}
	}
	
	private synchronized void initAllStrategyWorkers() {
		if (allStrategyWorkers == null) {
			
			if (allStrategies == null) {
				initAllStrategies();
				if (allStrategies == null) {
					throw new RuntimeException(
							"Invaild Monitor Strategy Conf");
				}
			}
			
			allStrategyWorkers = Collections.synchronizedMap(
					new HashMap<String, MonitorStrategyWorker>());
			Iterator<Map.Entry<String, MonitorStrategy>> iter =
				this.allStrategies.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, MonitorStrategy> entry =
					iter.next();
				String strategyName = entry.getKey();
				MonitorStrategy strategy = entry.getValue();
				MonitorStrategyWorker worker;
				try {
					worker = new MonitorStrategyWorker(strategy);
					//for (Date d:worker.getStrategy().getDataProcessingCentre().keySet()){
					//	System.out.println("kain's log see wether my item in : " + worker.getStrategy().getDataProcessingCentre().get(d).getInvolvedMonitorItemNames());
				//	}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				allStrategyWorkers.put(strategyName, worker);
			}
			if (allStrategyWorkers.size() == 0) {
				allStrategyWorkers = null;
			}
		}
	}
	
	private synchronized void launchMonitorStrategyWorkers() {
		if (allStrategyWorkers == null) {
			this.initAllStrategyWorkers();
			if (allStrategyWorkers == null) {
				throw new RuntimeException("Invaild Monitor Strategy Conf");
			}
		}
		Iterator<Map.Entry<String, MonitorStrategyWorker>> iter =
			this.allStrategyWorkers.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, MonitorStrategyWorker> entry = iter.next();
			MonitorStrategyWorker worker = entry.getValue();
			System.out.println("kain's log to see strategy : " + entry.getKey() + " " + entry.getValue().toString());
			strategyWorkerTimedScheduler.scheduleAtFixedRate(
					worker, rand.nextInt(60000), 60000);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		this.launchMonitorStrategyWorkers();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
