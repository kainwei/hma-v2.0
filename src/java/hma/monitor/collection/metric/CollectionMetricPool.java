/**
 * 
 */
package hma.monitor.collection.metric;

import hma.monitor.MonitorManager;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author guoyezhi
 *
 */
public class CollectionMetricPool {
	
	private static Queue<CollectionMetricRecord> pool = 
		new LinkedList<CollectionMetricRecord>();
	
	/*
	 * `MonitorManager' class is always loaded before `CollectionMetricPool' 
	 * class.
	 */
	private static boolean metricCollected = 
		MonitorManager.getGlobalConf().getBoolean(
				"monitor.collection.metric.enable", true);
	
	public synchronized static void offerRecord(CollectionMetricRecord record) {
		if (metricCollected) {
			// just discard record if not wanna collect
			pool.offer(record);
		}
	}
	
	public synchronized static CollectionMetricRecord pollRecord() {
		if (metricCollected) {
			return pool.poll();
		} else {
			return null;
		}
	}
	
}
