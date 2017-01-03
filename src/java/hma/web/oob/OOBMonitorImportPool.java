/**
 * 
 */
package hma.web.oob;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author guoyezhi
 *
 */
public class OOBMonitorImportPool {
	
	private static Queue<OOBMonitorImportRecord> pool =
		new LinkedList<OOBMonitorImportRecord>();
	
	public synchronized static void offerRecord(OOBMonitorImportRecord record) {
		pool.offer(record);
	}
	
	public synchronized static OOBMonitorImportRecord pollRecord() {
		return pool.poll();
	}
	
}
