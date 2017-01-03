/**
 * 
 */
package hma.monitor.collection.metric;

/**
 * @author guoyezhi
 *
 */
public class CollectionMetricRecord {
	
	private String clusterName = null;
	
	private String itemName = null;
	
	private long detectionTimestamp = 0;
	
	private long timeConsumed = -1;
	
	public CollectionMetricRecord(
			String clusterName, 
			String itemName, 			
			long detectionTimestamp, 
			long timeConsumed) {
		this.clusterName = clusterName;
		this.itemName = itemName;
		this.detectionTimestamp = detectionTimestamp;
		this.timeConsumed = timeConsumed;
	}
	
	public String getClusterName() {
		return clusterName;
	}
	
	public String getItemName() {
		return itemName;
	}
	
	public long getDetectionTimestamp() {
		return detectionTimestamp;
	}
	
	public long getTimeConsumed() {
		return timeConsumed;
	}
	
}
