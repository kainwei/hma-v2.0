/**
 * 
 */
package hma.web.oob;

/**
 * @author guoyezhi
 *
 */
public class OOBMonitorImportRecord {
	
	private String clusterName = null;
	
	private long detectionTimestamp = 0;
	
	private String OOBMonitorName = null;
	
	private String OOBMonitorResult = null;
	
	private String OOBMonitorLog = null;
	
	public OOBMonitorImportRecord(
			String clusterName,
			long detectionTimestamp,
			String OOBMonitorName,
			String OOBMonitorResult,
			String OOBMonitorLog) {
		this.clusterName = clusterName;
		this.detectionTimestamp = detectionTimestamp;
		this.OOBMonitorName = OOBMonitorName;
		this.OOBMonitorResult = OOBMonitorResult;
		this.OOBMonitorLog = OOBMonitorLog;
	}
	
	public String getClusterName() {
		return clusterName;
	}
	
	public long getDetectionTimestamp() {
		return detectionTimestamp;
	}
	
	public String getOOBMonitorName() {
		return OOBMonitorName;
	}
	
	public String getOOBMonitorResult() {
		return OOBMonitorResult;
	}
	
	public String getOOBMonitorLog() {
		return OOBMonitorLog;
	}
	
}
