/**
 * 
 */
package hma.monitor.strategy;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;
import hma.monitor.MonitorStrategy;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.strategy.MonitorStrategyPrototype.AnomalyDetectionStrategy;
import hma.monitor.strategy.MonitorStrategyPrototype.DataAcquisitionStrategy;
import hma.monitor.strategy.MonitorStrategyPrototype.TaskTriggerStrategy;
import hma.monitor.strategy.detect.AnomalyDetectionResult;
import hma.monitor.strategy.detect.AnomalyDetector;
import hma.monitor.strategy.trigger.TaskTrigger;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskAdapter;
import hma.monitor.strategy.trigger.task.MonitorStrategyAttachedTaskArguments;
import hma.util.HMALogger;
import hma.util.LOG;
import hma.util.TimedTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author guoyezhi
 * add additionalInfo by chiwen01
 *
 */
public class MonitorStrategyWorker extends TimedTask implements Runnable {
	
	private static String CLUSTER_NAME = null;
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd" + " " + "HH:mm:ss");
	
	private MonitorStrategy strategy = null;
	
	private String strategyName = null;
	
	private Date currentDataAcquisitionTimePoint = null;
	private long firstDataAcquisitionTimestamp = -1;
	private String detectionTimestampStr = null;
	
	private HMALogger logger = null;
	//private HMALogger adLogger = null;
	
	
	private String dbServer = null;
	private String dbName = null;
	private String resultTableName = null;
	private String infoTableName = null;
	private String userName = null;
	private String password = null;
	
	private Connection dbConn = null;
	//private Connection infTableConn = null;
	
	
	public MonitorStrategyWorker(MonitorStrategy strategy) throws SQLException {
		CLUSTER_NAME = MonitorManager.getMonitoredClusterName();
		this.strategy = strategy;
		this.strategyName = this.strategy.getPrototype().getName();
		this.defaultAlarmLevel = strategy.getPrototype().getAlarmLevel();
		this.logger = new HMALogger(strategyName + ".log");
		//this.adLogger = new HMALogger(strategyName + ".ad.log");
		this.initConnection();
	}
	
	private void initConnection() throws SQLException {
		
		Configuration hmaConf = 
			MonitorManager.getMonitorManager().getConf();
		
		dbServer = hmaConf.get(
				"monitor.data.db.server", "yf-2950-016.yf01.baidu.com");
		dbName = hmaConf.get(
				"monitor.data.db.name", "hma");
		resultTableName = hmaConf.get(
				"monitor.result.data.db.table", "monitor_result");
		infoTableName = hmaConf.get(
				"monitor.info.data.db.table", "monitor_info");
		userName = hmaConf.get(
				"monitor.db.user.name", "root");
		password = hmaConf.get(
				"monitor.db.user.pass", "hmainit");
		
                /*
		dbConn = DriverManager.getConnection(
				"jdbc:mysql://" + dbServer + "/" + dbName,
				userName, 
				password);
                */
		dbConn = DriverManager.getConnection(
				"jdbc:mysql://" + dbServer + "/" + dbName + "?user=" + userName  + "&password=" + password + "&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false");
		
		/*
		dbConn = DriverManager.getConnection(
				"jdbc:mysql://" + dbServer + "/" + dbName
					+ "?useUnicode=true&characterEncoding=utf8",
				userName, 
				password);
		*/
	}
	
	
	/**
	 * @param strategy the strategy to set
	 */
	public void setStrategy(MonitorStrategy strategy) {
		this.strategy = strategy;
	}
	
	/**
	 * @return the strategy
	 */
	public MonitorStrategy getStrategy() {
		return strategy;
	}
	
	/**
	 * @param currentDataAcquisitionTimePoint the currentDataAcquisitionTimePoint to set
	 */
	public void setDataAcquisitionTimepoint(Date dataAcquisitionTimePoint) {
		this.currentDataAcquisitionTimePoint = dataAcquisitionTimePoint;
	}
	
	/**
	 * @return the currentDataAcquisitionTimePoint
	 */
	public Date getDataAcquisitionTimepoint() {
		return currentDataAcquisitionTimePoint;
	}
	
	@SuppressWarnings("unchecked")
	private boolean acquireData() {
		
		long currentTime = System.currentTimeMillis();
		setDataAcquisitionTimepoint(new Date(currentTime));
		if (firstDataAcquisitionTimestamp < 0) {
			firstDataAcquisitionTimestamp = currentTime;
		}
		logger.log("[" + strategyName + "] : DataAcquisitionTimepoint: "
				+ getDataAcquisitionTimepoint());
		
		Map<String, MonitorItemData<?>> dataPool = getStrategy().getDataPool();
		NavigableMap<Date, MonitorStrategyDataCollection> dataProcessingCentre =
			getStrategy().getDataProcessingCentre();
		
		MonitorStrategyPrototype prototype = getStrategy().getPrototype();
		DataAcquisitionStrategy daStrategy = prototype.getDataAcquisitionStrategy();
		
		MonitorStrategyDataCollection dataCollection = 
			new MonitorStrategyDataCollection(prototype.getName());
		boolean acqFailed = false;
		String fullInfo = null;
		StringBuilder fullInfoBuilder = new StringBuilder();
		
		if (daStrategy.containsMonitorItem(DataAcquisitionStrategy.ALL_ITEMS_CONF_CONSTANT)) {
			
			long timeout = 
				daStrategy.getTimeoutMetaByMonitorItemName(
						DataAcquisitionStrategy.ALL_ITEMS_CONF_CONSTANT);
			
			List<MonitorStrategyAttachedTaskAdapter> adapterList = 
				daStrategy.getAttachedTaskAdaptersByMonitorItemName(
						DataAcquisitionStrategy.ALL_ITEMS_CONF_CONSTANT);
			
			StringBuilder resInfoBuilder = new StringBuilder();
            JSONObject timeoutItemNameJsonObject = new JSONObject();
			
			synchronized (dataPool) {
				Iterator<String> iter = dataPool.keySet().iterator();
				while (iter.hasNext()) {
					
					String itemName = iter.next();
					
					long timestamp = -1;
					
					MonitorItemData<?> itemData = dataPool.get(itemName);
					if (itemData == null) {
						/*
						 * If we get itemData == null here, it indicate relevant 
						 * collector/extractor hasn't got data ready for detection!
						 * 
						 * To prevent this situation lasting for a long time, we use 
						 * a fake data collection timestamp -- firstDataAcquisitionTimestamp. 
						 */
						timestamp = firstDataAcquisitionTimestamp;
					} else {
						timestamp = itemData.getTimestamp();
					}
					
					if ((timeout > 0) && (currentTime - timestamp >= timeout)) {
						acqFailed = true;
						alarmLevel = 
							MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
						resInfoBuilder.append(
								"\t监控项 " + itemName + " 数据采集超时 "
								+ ((currentTime - timestamp)) / 1000 + " 秒\n");
                        try {
                            timeoutItemNameJsonObject.put("additionalInfo", itemData.getAdditionalInfo());
                        } catch (JSONException e) {
                            e.getCause();
                        }
					} else if (itemData == null) {
						// nothing needed to do
					} else {
						dataCollection.put(itemName,
								new MonitorStrategyData(itemData));
					}
				}
			}
			
			if (acqFailed == true) {
				
				if (resInfoBuilder.length() > 0) {
					
					MonitorStrategyAttachedTaskArguments args =
						new MonitorStrategyAttachedTaskArguments(
								CLUSTER_NAME, 
								strategyName, 
								MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(alarmLevel), 
								"复合型监控项数据采集超时", 
								"复合型监控项数据采集超时 :\n" + resInfoBuilder.toString(), 
								getDataAcquisitionTimepoint(),
								timeoutItemNameJsonObject.toString());
					
					// trigger attached tasks
					for (MonitorStrategyAttachedTaskAdapter adapter : adapterList) {
						adapter.runTask(args);
					}
					
					fullInfo = "复合型监控项数据采集超时 :\n" + resInfoBuilder.toString();
				}
				
			} else {
				for (MonitorStrategyAttachedTaskAdapter adapter : adapterList) {
					adapter.clearContinuousOccurrence();
				}
			}
		}
		else {
			
			Iterator<String> iter = 
				daStrategy.getAllInvolvedItemNames().iterator();
			
			while (iter.hasNext()) {
				
				String itemName = iter.next();
				
				if (itemName.equals(DataAcquisitionStrategy.NO_ITEM_CONF_CONSTANT)) {
					/*
					 * If we get "#NO ITEM#" here, just skip it
					 */
					continue;
				}
				
				long timeout = 
					daStrategy.getTimeoutMetaByMonitorItemName(itemName);
				List<MonitorStrategyAttachedTaskAdapter> adapterList = 
					daStrategy.getAttachedTaskAdaptersByMonitorItemName(itemName);
				
				long timestamp = -1;
				MonitorItemData<?> itemData = null;
				
				synchronized (dataPool) {
					itemData = dataPool.get(itemName);
				}
				if (itemData == null) {
					/*
					 * If we get itemData == null here, it indicate relevant 
					 * collector/extractor hasn't got data ready for detection!
					 * 
					 * To prevent this situation lasting for a long time, we use 
					 * a fake data collection timestamp -- firstDataAcquisitionTimestamp. 
					 */
					timestamp = firstDataAcquisitionTimestamp;
				} else {
					timestamp = itemData.getTimestamp();
				}
				if ((timeout > 0) && (currentTime - timestamp >= timeout)) {
					
					acqFailed = true;
					alarmLevel = 
						MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_CRITICAL;
					
					MonitorStrategyAttachedTaskArguments args =
						new MonitorStrategyAttachedTaskArguments(
								CLUSTER_NAME, 
								strategyName, 
								MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(alarmLevel), 
								"监控项 " + itemName + " 数据采集超时 " + ((currentTime - timestamp)) / 1000 + " 秒",
								"监控项 " + itemName + " 数据采集超时 " + ((currentTime - timestamp)) / 1000 + " 秒",
								getDataAcquisitionTimepoint(),
								itemData.getAdditionalInfo());
					
					// trigger attached tasks
					for (MonitorStrategyAttachedTaskAdapter adapter : adapterList) {
						adapter.runTask(args);
					}
					
					fullInfoBuilder.append(
							"监控项 " + itemName + " 数据采集超时 " + ((currentTime - timestamp)) / 1000 + " 秒\n");
					
				} else if (itemData == null) {
					// nothing needed to do
				} else {
					for (MonitorStrategyAttachedTaskAdapter adapter : adapterList) {
						adapter.clearContinuousOccurrence();
					}
					dataCollection.put(itemName, new MonitorStrategyData(itemData));
				}
			}
			
			if (acqFailed == true) {
				if (daStrategy.getAllInvolvedItemNames().size() > 1) {
					fullInfo = "组合型监控项数据采集超时 :\n" + fullInfoBuilder.toString();
				} else {
					fullInfo = fullInfoBuilder.toString();
				}
			}
			
		}
		
		detectionTimestampStr = dateFormat.format(new Date());
		
		if (acqFailed == true) {
			String levelStr = 
				MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(
						alarmLevel);
			repositMonitorResult(detectionTimestampStr, levelStr, 1);
			repositMonitorAlarmInfo(detectionTimestampStr, levelStr, fullInfo);
			return false;
		}
		
		synchronized (dataProcessingCentre) {
			dataProcessingCentre.put(
					this.getDataAcquisitionTimepoint(), dataCollection);
		}
		
		return true;
	}
	
	
	private void repositMonitorResult(
			String detectionTimestamp,
			String strategyLevel,
			int detectionResult) {
		
		PreparedStatement pstat = null;
		try {
			pstat = dbConn.prepareStatement(
					"INSERT INTO " +  resultTableName + " VALUES ( "
					+ "'" + CLUSTER_NAME + "', "
					+ "'" + strategyName + "', "
					+ "?, ?, ? )");
			pstat.setString(1, detectionTimestamp);
			pstat.setString(2, strategyLevel);
			pstat.setInt(3, detectionResult);
			pstat.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstat != null) {
				try {
					pstat.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private void repositMonitorAlarmInfo(
			String detectionTimestamp,
			String infoLevel,
			String infoContent) {
		
		PreparedStatement pstat = null;
		try {
			pstat = dbConn.prepareStatement(
					"INSERT INTO " +  infoTableName + " VALUES ( "
					+ "'" + CLUSTER_NAME + "', "
					+ "'" + strategyName + "', "
					+ "?, ?, ? )");
			pstat.setString(1, detectionTimestamp);
			pstat.setString(2, infoLevel);
			pstat.setString(3, infoContent);
			System.out.println(pstat.toString());
			pstat.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstat != null) {
				try {
					pstat.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	
	private int defaultAlarmLevel = 
		MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_UNINIT;
	private int alarmLevel = 
		MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_UNINIT;
	private List<AnomalyDetectionResult> detectResults = null;
	
	private void detectAnomaly() {
		
		AnomalyDetectionStrategy adStrategy =
			getStrategy().getPrototype().getAnomalyDetectionStrategy();
		
		boolean anomalous = true;
		
		
		List<AnomalyDetector> detectors =
			adStrategy.getAllInvolvedAnomalyDetectors();
		for (AnomalyDetector detector : detectors) {
			AnomalyDetectionResult res = detector.detect(this);
			/*
			 * If return <b>null</b>, it indicates that the detection result is 
			 * not needed.
			 */
			if (res == null) {
				anomalous = false;
				continue;
			}
			
			detectResults.add(res);
			if (res.isAnomalous() == false) {
				anomalous = false;
				continue;
			}
			int newLevel = res.getAlarmLevel();
			if (alarmLevel < newLevel)
				alarmLevel = newLevel;
		}
		
		if (anomalous == true) {
			NavigableMap<Date, MonitorStrategyDataCollection> dataProcessingCentre =
				getStrategy().getDataProcessingCentre();
			synchronized (dataProcessingCentre) {
				dataProcessingCentre.get(
						getDataAcquisitionTimepoint()).setAnomalous(anomalous);
			}
		}
		
	}
	
	
	private void triggerAttachedTasks() {
		
		TaskTriggerStrategy ttStrategy = 
			getStrategy().getPrototype().getTaskTriggerStrategy();
		
		TaskTrigger taskTrigger = ttStrategy.getTaskTrigger();
		
		//System.out.println("mode = " + mode);
		//System.out.println("strategyConf = " + strategyConf);
		
		List<MonitorStrategyAttachedTaskAdapter> adapterList = 
			ttStrategy.getAttachedTaskAdapters();
		
		if (taskTrigger.visitMonitorStrategyWorker(this) == true) {
			
			String keyInfo = null;
			StringBuilder keyInfoBuilder = new StringBuilder();
			String fullInfo = null;
			StringBuilder fullInfoBuilder = new StringBuilder();
            String additionalInfo = null;
            StringBuilder additionalInfoBuilder = new StringBuilder();
			
			/*
			 * TODO: keyInfo = prototype.getKeyAlarmMessage();
			 */
			for (AnomalyDetectionResult res : detectResults) {
				
				String alarmInfo = res.getAlarmInfo();
				int index = alarmInfo.indexOf("\n");
				if (index > 0)
					alarmInfo = alarmInfo.substring(0, index);
				keyInfoBuilder.append(alarmInfo + " ");
				
				fullInfoBuilder.append(res.getAlarmInfo());

                additionalInfoBuilder.append(res.getAdditionalInfo());
			}
			if (keyInfo == null)
				keyInfo = keyInfoBuilder.toString();
			fullInfo = fullInfoBuilder.toString();
            additionalInfo = additionalInfoBuilder.toString();
			
			MonitorStrategyAttachedTaskArguments args = 
				new MonitorStrategyAttachedTaskArguments(
						CLUSTER_NAME, 
						strategyName, 
						MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(
								alarmLevel), 
						keyInfo, 
						fullInfo, 
						new Date(),
						additionalInfo);
			
			for (MonitorStrategyAttachedTaskAdapter adapter : adapterList) {
				adapter.runTask(args);
			}
			
			String levelStr = 
				MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(
						alarmLevel);
			repositMonitorResult(detectionTimestampStr, levelStr, 1);
			repositMonitorAlarmInfo(detectionTimestampStr, levelStr, fullInfo);
			
		} else {
			for (MonitorStrategyAttachedTaskAdapter adapter : adapterList) {
				adapter.clearContinuousOccurrence();
			}
			
			String levelStr = 
				MonitorStrategyAttachedTaskAdapter.getAlarmLevelStringFromValue(
						alarmLevel);
			repositMonitorResult(detectionTimestampStr, levelStr, 0);
			
		} // if (taskTrigger.visitMonitorStrategyWorker(this) == true)
		
	}
	
	
	private static final int DATA_INMEM_MAX_CYCLE = 100;
	
	private void resizeDataProcessingCentre() {
		NavigableMap<Date, MonitorStrategyDataCollection> dataProcessingCentre =
			getStrategy().getDataProcessingCentre();
		synchronized (dataProcessingCentre) {
			while (dataProcessingCentre.size() > DATA_INMEM_MAX_CYCLE) {
				dataProcessingCentre.pollFirstEntry();
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see hma.util.TimedTask#run()
	 */
	@Override
	public synchronized void run() {
		
		alarmLevel = defaultAlarmLevel;
		detectResults = new ArrayList<AnomalyDetectionResult>();
		
		//LOG.debug("[" + strategyName + "] : acquiring data ...");
		logger.log("[" + strategyName + "] : acquiring data ...");
		if (this.acquireData() == false)
			return;
		
		LOG.debug("[" + strategyName + "] : detecting anomaly ...");
		logger.log("[" + strategyName + "] : detecting anomaly ...");
		alarmLevel = 
			MonitorStrategyAttachedTaskAdapter.ALARM_LEVEL_UNINIT;
		detectAnomaly();
		
		//LOG.debug("[" + strategyName + "] : triggering tasks ...");
		logger.log("[" + strategyName + "] : triggering tasks ...");
		triggerAttachedTasks();
		
		logger.log("[" + strategyName + "] : resizing data processing centre ...");
		resizeDataProcessingCentre();
		
		//LOG.debug("************************************************************");
		logger.log("************************************************************");
		
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String name = "SimulationMonitorStrategy";
		
		MonitorStrategyConfiguration conf = new MonitorStrategyConfiguration();
		MonitorStrategyPrototype prototype = conf.getAllPrototypes().get(name);
		System.out.println(prototype);
		
		MonitorStrategy strategy = new MonitorStrategy(prototype, null);
		
		LOG.warn("---> Anomaly Detectors:");
		AnomalyDetectionStrategy adStrategy =
			strategy.getPrototype().getAnomalyDetectionStrategy();
		List<AnomalyDetector> detectors =
			adStrategy.getAllInvolvedAnomalyDetectors();
		for (AnomalyDetector detector : detectors) {
			LOG.warn(detector + "");
		}
		
		TaskTriggerStrategy ttStrategy = 
			strategy.getPrototype().getTaskTriggerStrategy();
		TaskTrigger visitor = ttStrategy.getTaskTrigger();
		System.out.println(visitor);
	}

}
