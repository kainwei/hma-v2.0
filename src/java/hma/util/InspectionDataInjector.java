package hma.util;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class InspectionDataInjector {
	
	
	private String dbServer = null;
	private String dbName = null;
	private String inspectionTable = null;

	private String userName = null;
	private String password = null;
	private Connection dbConn = null;
	
	
	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd" + " " + "HH:mm:ss");
	
    private void initConnection() throws SQLException {
		
		Configuration hmaConf = 
			MonitorManager.getMonitorManager().getConf();
		
		dbServer = hmaConf.get(
				"monitor.data.db.server", "yf-2950-016.yf01.baidu.com");
		dbName = hmaConf.get(
				"monitor.data.db.name", "hma");
	
	   inspectionTable = hmaConf.get(
				"monitor.inspection.data.db.table", "monitor_inspection");
		userName = hmaConf.get(
				"monitor.db.user.name", "root");
		password = hmaConf.get(
				"monitor.db.user.pass", "hmainit");
		
		dbConn = DriverManager.getConnection(
				"jdbc:mysql://" + dbServer + "/" + dbName,
				userName, 
				password);
		
		/*
		dbConn = DriverManager.getConnection(
				"jdbc:mysql://" + dbServer + "/" + dbName
					+ "?useUnicode=true&characterEncoding=utf8",
				userName, 
				password);
		*/
	}

    public  InspectionDataInjector (String clusterName, 
		                             String strategyName,
		                             String detectionTimestamp,
		                             String infoLevel,
		                             String infoContent) throws SQLException, ParseException {
    	this.initConnection();

    	PreparedStatement pstat = null;
    	String oldTime = dateFormat.format(new Date(dateFormat.parse(detectionTimestamp).getTime() - 3600*1000) );

        //System.out.println("kain's inspection's log in Injector : " + clusterName + " " + strategyName + " " + detectionTimestamp + " " + infoLevel + " " + infoContent + oldTime);
    	
    	pstat = dbConn.prepareStatement("DELETE FROM " + inspectionTable + " WHERE detection_timestamp<" + "'" + oldTime + "'");
    	//System.out.println(pstat.toString());
    	pstat.executeUpdate();
    	
    	pstat = dbConn.prepareStatement(
				"INSERT INTO " +  inspectionTable + " VALUES ( ?, ?, ?, ?, ?)");
		pstat.setString(1, clusterName);
		pstat.setString(2, strategyName);
		pstat.setString(3, detectionTimestamp);
		pstat.setString(4, infoLevel);
		pstat.setString(5, infoContent);
		//System.out.println(pstat.toString());
		pstat.executeUpdate();
		
		pstat.close();
	
	
    }

}
