/**
 * 
 */
package hma.web.oob;

import hma.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author guoyezhi
 *
 */
public class OOBMonitorImportDBDumper implements Runnable {
	
	private Configuration hmaConf = null;
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd" + " " + "HH:mm:ss");
	
	private String dbServer = null;
	private String dbName = null;
	private String tableName = null;
	private String userName = null;
	private String password = null;
	
	
	public OOBMonitorImportDBDumper(Configuration hmaConf) {
		this.hmaConf = hmaConf;
	}
	
	
	private Connection initConnection() throws SQLException {
		
		dbServer = hmaConf.get(
				"monitor.data.db.server", "yf-2950-hma-db.yf01.baidu.com");
		dbName = hmaConf.get(
				"monitor.data.db.name", "hma");
		tableName = hmaConf.get(
				"monitor.extension.oob.monitor.imports.table",
				"monitor_ext_oobmonimports");
		userName = hmaConf.get(
				"monitor.db.user.name", "root");
		password = hmaConf.get(
				"monitor.db.user.pass", "hmainit");
		
		return DriverManager.getConnection(
				"jdbc:mysql://" + dbServer + "/" + dbName,
				userName, 
				password);
	}
	
	@Override
	public void run() {
		
		Connection dbConn = null;
		try {
			dbConn = this.initConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
		while (true) {
			OOBMonitorImportRecord record = 
				OOBMonitorImportPool.pollRecord();
			if (record == null) {
				try {
					TimeUnit.SECONDS.sleep(60);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			PreparedStatement pstat = null;
			try {
				String detectionTimestamp = 
					dateFormat.format(new Date(record.getDetectionTimestamp()));
				pstat = dbConn.prepareStatement(
						"INSERT INTO " +  tableName + " VALUES ( ?, ?, ?, ?, ? )");
				pstat.setString(1, record.getClusterName());
				pstat.setString(2, detectionTimestamp);
				pstat.setString(3, record.getOOBMonitorName());
				if (record.getOOBMonitorResult().equals("normal")) {
					pstat.setInt(4, 0); // 0: normal
				} else {
					pstat.setInt(4, 1); // 1: abnormal
				}
				pstat.setString(5, record.getOOBMonitorLog());
				pstat.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if (pstat != null) {
					try {
						pstat.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
}
