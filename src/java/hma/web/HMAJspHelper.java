/**
 *
 */
package hma.web;

import hma.conf.Configuration;
import hma.monitor.collection.metric.CollectionMetricRecord;

import hma.monitor.strategy.MonitorStrategyConfiguration;
import hma.monitor.strategy.MonitorStrategyPrototype;
import hma.util.HMALogger;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * @author guoyezhi
 */
public class HMAJspHelper {

    private String dbServer = null;
    private String dbName = null;

    private String resultTableName = null;
    private String infoTableName = null;

    private String collectionMetricTableName = null;

    /**
     * DB Table for storing information about Unworking Datanodes
     */
    private String extUDNTableName = null;
    /**
     * DB Table for storing information about User INode Quota Info
     */
    private String extUserINodeQuotaInfoTableName = null;
    /**
     * DB Table for storing information about User INode Quota Info
     */
    private String extUserSpaceQuotaInfoTableName = null;
    /**
     * DB Table for OOB monitor imports.
     */
    private String extOOBMonitorImportsTableName = null;

    /**
     * Inspection Table for daily inspection
     */
    private String inspectionTable = null;

    private String userName = null;
    private String password = null;

    private static Configuration clusterStatConf =
            new Configuration(true, "hma-clusterhealth");
    private static MonitorStrategyConfiguration strategyConf =
            new MonitorStrategyConfiguration(true);
    private static Map<String, String> strategyName2AliasMapping = null;

    private static SimpleDateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public HMAJspHelper() {

        Configuration hmaConf = new Configuration(true, "hma");

        dbServer = hmaConf.get(
                "monitor.data.db.server", "yf-2950-016.yf01.baidu.com");
        dbName = hmaConf.get(
                "monitor.data.db.name", "hma");

        resultTableName = hmaConf.get(
                "monitor.result.data.db.table", "monitor_result");
        infoTableName = hmaConf.get(
                "monitor.info.data.db.table", "monitor_info");
        collectionMetricTableName = hmaConf.get(
                "monitor.collection.metric.db.table",
                "monitor_cmetric");
        extUDNTableName = hmaConf.get(
                "monitor.extension.unworkingdatanodes.info.db.table",
                "monitor_ext_unworkingdatanodes");
        extUserINodeQuotaInfoTableName = hmaConf.get(
                "monitor.extension.user.inode.quota.db.table",
                "monitor_ext_userinodequota");
        extUserSpaceQuotaInfoTableName = hmaConf.get(
                "monitor.extension.user.space.quota.db.table",
                "monitor_ext_userspacequota");
        extOOBMonitorImportsTableName = hmaConf.get(
                "monitor.extension.oob.monitor.imports.table",
                "monitor_ext_oobmonimports");
        inspectionTable = hmaConf.get(
                "monitor.inspection.data.db.table", "monitor_inspection");

        userName = hmaConf.get(
                "monitor.db.user.name", "root");
        password = hmaConf.get(
                "monitor.db.user.pass", "hmainit");

    }


    public static synchronized boolean isMonitorStrategyTransient(
            String strategyName) {
        String str = clusterStatConf.get(strategyName, "permanent");
        if (str.toLowerCase().equals("transient"))
            return true;
        return false;
    }

    public static synchronized HashMap<String, MonitorStrategyPrototype>
    getAllMonitorStrategyPrototypes() {
        return strategyConf.getAllPrototypes();
    }

    public static synchronized MonitorStrategyPrototype
    getMonitorStrategyPrototype(String strategyName) {
        return strategyConf.getAllPrototypes().get(strategyName);
    }

    public static synchronized String getMonitorStrategyAlias(String strategyName) {
        if (strategyName2AliasMapping == null) {
            strategyName2AliasMapping = new HashMap<String, String>();
            HashMap<String, MonitorStrategyPrototype> entries =
                    strategyConf.getAllPrototypes();
            Iterator<String> iter = entries.keySet().iterator();
            while (iter.hasNext()) {
                String name = iter.next();
                String alias = entries.get(name).getAlias();
                strategyName2AliasMapping.put(name, alias);
            }
        }
        return strategyName2AliasMapping.get(strategyName);
    }

    public static class StrategyComparator
            implements Comparator<MonitorStrategyPrototype> {

        @Override
        public int compare(MonitorStrategyPrototype arg0,
                           MonitorStrategyPrototype arg1) {
            if(arg0 == null || arg1 == null ){
                return 0;
            }
            System.err.println(arg0.getName() + " vs. " + arg1.getName());
            int level0 = arg0.getAlarmLevel();
            int level1 = arg1.getAlarmLevel();
            if (level0 > level1) {
                return -1;
            } else if (level0 < level1) {
                return 1;
            }
            String name0 = arg0.getName();
            String alias0 = arg0.getAlias();
            if (alias0 != null) name0 = alias0;
            String name1 = arg1.getName();
            String alias1 = arg1.getAlias();
            if (alias1 != null) name1 = alias1;
            return name0.compareTo(name1);
        }

    }


    private Connection createDBConnection() throws SQLException {

        System.out.println(
                "jdbc:mysql://" + dbServer + "/" + dbName +
                        "/" + userName + "@" + password);

        return DriverManager.getConnection(
                "jdbc:mysql://" + dbServer + "/" + dbName,
                userName,
                password);

		/*
        return DriverManager.getConnection(
				"jdbc:mysql://" + dbServer + "/" + dbName 
					+ "?useUnicode=true&characterEncoding=utf8",
				userName, 
				password);
		*/
    }


    public static final int DetectionResult_NODATA = 0x00;
    public static final int DetectionResult_NORMAL = 0x01;
    public static final int DetectionResult_HASINFO = 0x02;
    public static final int DetectionResult_NOTICEABLE = 0x03;
    public static final int DetectionResult_ANORMLOUS = 0x04;

    public synchronized Map<String, Integer> checkDetectionResults(
            String clusterName,
            String begStr,
            String endStr) throws SQLException {

        Map<String, Integer> checkResult = new HashMap<String, Integer>();

        Connection dbConn = createDBConnection();

        Iterator<Map.Entry<String, String>> iter =
                clusterStatConf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String strategyName = entry.getKey();
            boolean permanent =
                    entry.getValue().toLowerCase().equals("permanent");

            PreparedStatement pstat = null;
            if (permanent == false) {
                pstat = dbConn.prepareStatement(
                        "SELECT detection_timestamp, strategy_level, detection_result " +
                                "FROM " + resultTableName + " " +
                                "WHERE cluster_name=? " +
                                "AND strategy_name=? " +
                                "AND detection_timestamp>? " +
                                "AND detection_timestamp<? " +
                                "ORDER BY detection_timestamp DESC limit 1");
                pstat.setString(1, clusterName);
                pstat.setString(2, strategyName);
                pstat.setString(3, begStr);
                pstat.setString(4, endStr);
                ResultSet res = pstat.executeQuery();
                if (res.next()) {
                    Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
                    String strategyLevel = res.getString("strategy_level");
                    boolean anomalous = res.getBoolean("detection_result");
                    System.out.println(detectionTimestamp + " " + anomalous);
                    if (anomalous) {
                        if (strategyLevel.toUpperCase().equals("INFO")) {
                            checkResult.put(strategyName, DetectionResult_HASINFO);
                        } else if (strategyLevel.toUpperCase().equals("NOTICE")) {
                            checkResult.put(strategyName, DetectionResult_NOTICEABLE);
                        } else {
                            checkResult.put(strategyName, DetectionResult_ANORMLOUS);
                        }
                    } else {
                        checkResult.put(strategyName, DetectionResult_NORMAL);
                    }
                } else {
                    checkResult.put(strategyName, DetectionResult_NODATA);
                }

            } else {
                pstat = dbConn.prepareStatement(
                        "SELECT detection_timestamp, strategy_level, detection_result " +
                                "FROM " + resultTableName + " " +
                                "WHERE cluster_name=? " +
                                "AND strategy_name=? " +
                                "AND detection_timestamp>? " +
                                "AND detection_timestamp<=? " +
                                "ORDER BY detection_timestamp ASC");
                pstat.setString(1, clusterName);
                pstat.setString(2, strategyName);
                pstat.setString(3, begStr);
                pstat.setString(4, endStr);
                ResultSet res = pstat.executeQuery();

                boolean anomalous = false;
                boolean notice = true;
                boolean info = true;

                int cnt = 0;
                for (; res.next(); cnt++) {
                    anomalous = anomalous || res.getBoolean("detection_result");
                    if (res.getBoolean("detection_result")) {
                        String level = res.getString("strategy_level").toUpperCase();
                        if (level.equals("EMERG") || level.equals("CRITICAL")
                                || level.equals("WARN") || level.equals("WARNING")) {
                            notice = false;
                            info = false;
                        } else if (level.equals("NOTICE")) {
                            info = false;
                        }
                    }
                }
                if (cnt > 0) {
                    if (anomalous) {
                        if (info) {
                            checkResult.put(strategyName, DetectionResult_HASINFO);
                        } else if (notice) {
                            checkResult.put(strategyName, DetectionResult_NOTICEABLE);
                        } else {
                            checkResult.put(strategyName, DetectionResult_ANORMLOUS);
                        }
                    } else {
                        checkResult.put(strategyName, DetectionResult_NORMAL);
                    }
                } else {
                    checkResult.put(strategyName, DetectionResult_NODATA);
                }
            }
        }

        return checkResult;
    }

    public synchronized Map<String, Integer> checkOOBMonitorImportResults(
            String clusterName,
            String begStr,
            String endStr) throws SQLException {

        Map<String, Integer> checkResult = new HashMap<String, Integer>();

        Connection dbConn = createDBConnection();

        PreparedStatement pstat = null;
        pstat = dbConn.prepareStatement(
                "SELECT DISTINCT oob_monitor_name " +
                        "FROM " + extOOBMonitorImportsTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND detection_timestamp>? " +
                        "AND detection_timestamp<=? " +
                        "ORDER BY oob_monitor_name");
        pstat.setString(1, clusterName);
        pstat.setString(2, begStr);
        pstat.setString(3, endStr);
        ResultSet res = pstat.executeQuery();
        while (res.next()) {
            checkResult.put(res.getString("oob_monitor_name"),
                    DetectionResult_NORMAL);
        }
        pstat.close();

        for (String oobMonitorName : checkResult.keySet()) {
            pstat = dbConn.prepareStatement(
                    "SELECT detection_timestamp, oob_monitor_result " +
                            "FROM " + extOOBMonitorImportsTableName + " " +
                            "WHERE cluster_name=? " +
                            "AND oob_monitor_name=? " +
                            "AND detection_timestamp>? " +
                            "AND detection_timestamp<=? " +
                            "ORDER BY detection_timestamp ASC");
            pstat.setString(1, clusterName);
            pstat.setString(2, oobMonitorName);
            pstat.setString(3, begStr);
            pstat.setString(4, endStr);
            res = pstat.executeQuery();

            boolean anomalous = false;
            int cnt = 0;
            for (; res.next(); cnt++) {
                anomalous = anomalous || res.getBoolean("oob_monitor_result");
            }
            if (cnt > 0) {
                if (anomalous) {
                    checkResult.put(oobMonitorName, DetectionResult_ANORMLOUS);
                } else {
                    checkResult.put(oobMonitorName, DetectionResult_NORMAL);
                }
            } else {
                checkResult.put(oobMonitorName, DetectionResult_NODATA);
            }
            pstat.close();
        }

        return checkResult;
    }


    public static class MonitorStrategyAnomaly {

        private String clusterName = null;

        private String strategyName = null;

        private Long detectionTimestamp = null;

        private String infoLevel = null;

        private String infoContent = null;

        public MonitorStrategyAnomaly(
                Long detectionTimestamp,
                String infoLevel,
                String infoContent) {
            this.detectionTimestamp = detectionTimestamp;
            this.infoLevel = infoLevel;
            this.infoContent = infoContent;
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getStrategyName() {
            return strategyName;
        }

        public Long getDetectionTimestamp() {
            return detectionTimestamp;
        }

        public String getInfoLevel() {
            return infoLevel;
        }

        public String getInfoContent() {
            return infoContent;
        }
    }

    public synchronized MonitorStrategyAnomaly getLastRecentMonitorStrategyAnomaly(
            String clusterName,
            String strategyName) throws SQLException {
        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT detection_timestamp, info_level, info_content " +
                        "FROM " + infoTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND strategy_name=? " +
                        "ORDER BY detection_timestamp DESC limit 1");
        pstat.setString(1, clusterName);
        pstat.setString(2, strategyName);
        ResultSet res = pstat.executeQuery();
        if (res.next()) {
            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            String infoLevel = res.getString("info_level");
            String infoContent = res.getString("info_content");
            return new MonitorStrategyAnomaly(
                    detectionTimestamp.getTime(), infoLevel, infoContent);
        }
        return null;
    }

    public synchronized NavigableMap<Long, MonitorStrategyAnomaly>
    getMonitorStrategyAnomaliesWithinPeriod(
            String clusterName,
            String strategyName,
            long begginning, long end) throws SQLException {

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT detection_timestamp, info_level, info_content " +
                        "FROM " + infoTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND strategy_name=? " +
                        "AND detection_timestamp>? " +
                        "AND detection_timestamp<=? " +
                        "ORDER BY detection_timestamp");
        pstat.setString(1, clusterName);
        pstat.setString(2, strategyName);
        pstat.setString(3, dateFormat.format(new Date(begginning)));
        pstat.setString(4, dateFormat.format(new Date(end)));
        ResultSet res = pstat.executeQuery();

        NavigableMap<Long, MonitorStrategyAnomaly> anomalies =
                new TreeMap<Long, MonitorStrategyAnomaly>();

        while (res.next()) {
            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            String infoLevel = res.getString("info_level");
            String infoContent = res.getString("info_content");
            anomalies.put(detectionTimestamp.getTime(),
                    new MonitorStrategyAnomaly(
                            detectionTimestamp.getTime(),
                            infoLevel, infoContent));
        }

        return anomalies;
    }

    public synchronized NavigableMap<Long, String>
    getOOBMonitorLogsWithinPeriod(
            String clusterName,
            String oobMonitorName,
            long begginning,
            long end) throws SQLException {

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT detection_timestamp, oob_monitor_log " +
                        "FROM " + extOOBMonitorImportsTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND oob_monitor_name=? " +
                        "AND oob_monitor_result=? " +
                        "AND detection_timestamp>? " +
                        "AND detection_timestamp<=? " +
                        "ORDER BY detection_timestamp");
        pstat.setString(1, clusterName);
        pstat.setString(2, oobMonitorName);
        pstat.setBoolean(3, true);
        pstat.setString(4, dateFormat.format(new Date(begginning)));
        pstat.setString(5, dateFormat.format(new Date(end)));
        ResultSet res = pstat.executeQuery();

        NavigableMap<Long, String> oobMonitorLogs = new TreeMap<Long, String>();

        while (res.next()) {
            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            String log = res.getString("oob_monitor_log");
            oobMonitorLogs.put(detectionTimestamp.getTime(), log);
        }

        return oobMonitorLogs;
    }

    public static void main(String[] args) throws SQLException, UnsupportedEncodingException {

        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://yf-2950-hma-db.yf01.baidu.com/hma?useUnicode=true&characterEncoding=utf8",
                "root", "hmainit");
        PreparedStatement pstat = conn.prepareStatement(
                "SELECT detection_timestamp, info_level, info_content " +
                        "FROM monitor_info " +
                        "WHERE cluster_name=? " +
                        "AND strategy_name=? " +
                        "AND detection_timestamp>? " +
                        "AND detection_timestamp<=? " +
                        "ORDER BY detection_timestamp LIMIT 1");
        pstat.setString(1, "ECOM-OFF");
        pstat.setString(2, "HDFS_RsyncLastMinuteFsimage_MON");
        pstat.setString(3, "2011-01-11 22:38:10");
        pstat.setString(4, "2011-01-11 22:58:10");
        ResultSet res = pstat.executeQuery();
        if (res.next()) {
            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            String infoLevel = res.getString("info_level");
            String infoContent = res.getString("info_content");
            System.out.println(detectionTimestamp + " : " + infoLevel + " : " + infoContent);
            byte[] bytes = res.getBytes("info_content");
            for (int i = 0; i < bytes.length; i++) {
                System.out.print(Integer.toHexString(bytes[i]) + " ");
            }
            System.out.println();
            System.out.println(new String(bytes, "utf-8"));
        }
        /*
		String str = "监控项 HDFS_RsyncLastMinuteFsimage_MON 数据采集超时 777 秒\n";
		byte[] bytes2 = str.getBytes("utf-8");
		for (int i = 0; i < bytes2.length; i++) {
			System.out.print(Integer.toHexString(bytes2[i]) + " ");
		}
		System.out.println();
		
		bytes2 = str.getBytes("utf-16");
		for (int i = 0; i < bytes2.length; i++) {
			System.out.print(Integer.toHexString(bytes2[i]) + " ");
		}
		*/

        HMALogger logger = new HMALogger("hmalogger.txt");
		
		/*
		pstat = conn.prepareStatement("INSERT INTO monitor_info " +
				"VALUES(\"Test\", \"Foo_MON\", \"2011-01-12 22:40:50\", \"WARN\", \"测试\")");
		System.out.println(pstat.toString());
		pstat.execute();
		*/

        pstat = conn.prepareStatement(
                "SELECT detection_timestamp, info_level, info_content " +
                        "FROM monitor_info WHERE cluster_name=\"ECOM-OFF\" " +
                        "ORDER BY detection_timestamp DESC LIMIT 1");
        res = pstat.executeQuery();
        if (res.next()) {
            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            String infoLevel = res.getString("info_level");
            String infoContent = res.getString("info_content");
            System.out.println(detectionTimestamp + " : " + infoLevel + " : " + infoContent);
            logger.log(detectionTimestamp + " : " + infoLevel + " : " + infoContent);

            Blob bl = res.getBlob("info_content");
            byte[] bytes = bl.getBytes(1, (int) bl.length());
            for (int i = 0; i < bytes.length; i++) {
                System.out.print(Integer.toHexString(bytes[i]) + " ");
            }
            System.out.println();
            System.out.println(new String(bytes));
            System.out.println(new String(bytes, "utf-8"));
        }
    }


    /*
     * The following fields & methods are for `unworkingdatanodes.jsp'
     */
    public static class DatanodeInfoComparator implements Comparator<DatanodeInfo> {

        @Override
        public int compare(DatanodeInfo arg0, DatanodeInfo arg1) {
            long timestamp0 = arg0.getLastUpdate();
            long timestamp1 = arg1.getLastUpdate();
            if (timestamp0 > timestamp1) {
                return -1;
            } else if (timestamp0 < timestamp1) {
                return 1;
            }
            String name0 = arg0.getHostName();
            String name1 = arg1.getHostName();
            return name0.compareTo(name1);
        }

    }

   /* public synchronized NavigableMap<Long, List<DatanodeInfo>> getClusterUnworkingDatanodeInfo(
            String clusterName) throws SQLException {

        TreeMap<Long, List<DatanodeInfo>> monitorInfo =
                new TreeMap<Long, List<DatanodeInfo>>();

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT * FROM " + extUDNTableName + " " +
                        "WHERE cluster_name=? " +
                        "ORDER BY detection_timestamp DESC " +
                        "LIMIT 1");
        pstat.setString(1, clusterName);
        ResultSet res = pstat.executeQuery();

        if (res.next()) {

            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            Blob blob = res.getBlob("serial_info");
            DataInputStream dataIS =
                    new DataInputStream(blob.getBinaryStream());
            try {
                List<DatanodeInfo> unworkingList = new ArrayList<DatanodeInfo>();

                int recreateLength = dataIS.readInt();
                for (int i = 0; i < recreateLength; i++) {
                    DatanodeInfo di = new DatanodeInfo();
                    di.readFields(dataIS);
                    unworkingList.add(di);
                }

                monitorInfo.put(detectionTimestamp.getTime(), unworkingList);

            } catch (IOException e) {
                e.printStackTrace();
                return null;
            } finally {
                try {
                    dataIS.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        pstat.close();
        dbConn.close();

        return monitorInfo;
    }*/


    /*public synchronized int getClusterUnworkingDatanodeNum(*/
    /*        String clusterName, long starttime, long endtime) throws SQLException {*/
/*
*/

/*
*/

    /*    Connection dbConn = createDBConnection();*/
    /*    PreparedStatement pstat = dbConn.prepareStatement(*/
    /*            "SELECT * FROM " + extUDNTableName + " " +*/
    /*                    "WHERE cluster_name=? " +*/
    /*                    "ORDER BY detection_timestamp DESC " +*/
    /*                    "LIMIT 1");*/
    /*    pstat.setString(1, clusterName);*/
    /*    ResultSet res = pstat.executeQuery();*/
    /*    int count = 0;*/
    /*    if (res.next()) {*/
/*
*/

/*
*/

    /*        Blob blob = res.getBlob("serial_info");*/
    /*        DataInputStream dataIS =*/
    /*                new DataInputStream(blob.getBinaryStream());*/
    /*        try {*/
/*
*/

/*
*/

    /*            int recreateLength = dataIS.readInt();*/
    /*            for (int i = 0; i < recreateLength; i++) {*/
    /*                DatanodeInfo di = new DatanodeInfo();*/
    /*                di.readFields(dataIS);*/
    /*                //unworkingList.add(di);*/
    /*                long time = di.getLastUpdate();*/
    /*                if (time >= starttime && time <= endtime && !di.isDecommissioned()) {*/
    /*                    count++;*/
    /*                }*/
    /*            }*/
    /*        } catch (IOException e) {*/
    /*            e.printStackTrace();*/
    /*            return count;*/
    /*        } finally {*/
    /*            try {*/
    /*                dataIS.close();*/
    /*            } catch (IOException e) {*/
    /*                e.printStackTrace();*/
    /*            }*/
    /*        }*/
    /*    }*/
/*
*/

    /*    pstat.close();*/
    /*    dbConn.close();*/
/*
*/

    /*    return count;*/
    /*}*/


    /*
     * The following fields & methods are for `userstatus.jsp'
     */
    public static class UserINodeQuotaInfo {

        private String userName = null;

        private String userAppPath = null;

        private long inodeQuota = -1L;

        private long inodeFileCount = 0L;

        private long inodeDirectoryCount = 0L;

        private Date detectionTimestamp = null;

        private double inodeAlarmThreshold = 80.0;

        public UserINodeQuotaInfo(
                String userName,
                String userAppPath,
                long inodeQuota,
                long inodeFileCount,
                long inodeDirectoryCount,
                Date detectionTimestamp,
                double inodeAlarmThreshold) {
            this.userName = userName;
            this.userAppPath = userAppPath;
            this.inodeQuota = inodeQuota;
            this.inodeFileCount = inodeFileCount;
            this.inodeDirectoryCount = inodeDirectoryCount;
            this.detectionTimestamp = detectionTimestamp;
            this.inodeAlarmThreshold = inodeAlarmThreshold;
        }

        public String getUserName() {
            return userName;
        }

        public String getUserAppPath() {
            return userAppPath;
        }

        public long getInodeQuota() {
            return inodeQuota;
        }

        public long getInodeFileCount() {
            return inodeFileCount;
        }

        public long getInodeDirectoryCount() {
            return inodeDirectoryCount;
        }

        public Date getDetectionTimestamp() {
            return detectionTimestamp;
        }

        public double getInodeAlarmThreshold() {
            return inodeAlarmThreshold;
        }

    }

    public static class UserINodeQuotaInfoComparator
            implements Comparator<UserINodeQuotaInfo> {

        @Override
        public int compare(UserINodeQuotaInfo info0, UserINodeQuotaInfo info1) {

            String userName0 = info0.getUserName();
            String userName1 = info1.getUserName();

            if (userName0.compareTo(userName1) != 0)
                return userName0.compareTo(userName1);

            String userAppPath0 = info0.getUserAppPath();
            String userAppPath1 = info1.getUserAppPath();

            return userAppPath0.compareTo(userAppPath1);
        }

    }

    /*public synchronized NavigableSet<UserINodeQuotaInfo>
    getAllUserINodeQuotaInfo(String clusterName) throws SQLException {

        NavigableSet<UserINodeQuotaInfo> monitorInfo =
                new TreeSet<UserINodeQuotaInfo>(new UserINodeQuotaInfoComparator());

        Configuration hmaConf = new Configuration(true, "hma");
        String confBaseDirPath = hmaConf.get(
                "hma.conf.base.dir", "/home/monitor/hma-v1.1/hma/conf/");
        if (!confBaseDirPath.endsWith("/")) {
            confBaseDirPath = confBaseDirPath + "/";
        }
        String defaultConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-default.xml";
        String siteConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-site.xml";
        File defaultConfFile = new File(defaultConfFilePath);
        File siteConfFile = new File(siteConfFilePath);
        if (!defaultConfFile.exists() && !siteConfFile.exists()) {
            throw new RuntimeException();
        }

        UserQuotaConfiguration quotaConf =
                new UserQuotaConfiguration(false);
        if (defaultConfFile.exists()) {
            quotaConf.addConfigurationResource(defaultConfFile);
        }
        if (siteConfFile.exists()) {
            quotaConf.addConfigurationResource(siteConfFile);
        }
        NavigableSet<UserQuotaConfigurationEntry> confEntries =
                quotaConf.loadEntries();

        Connection dbConn = createDBConnection();
        for (UserQuotaConfigurationEntry entry : confEntries) {

            System.out.println(entry);

            String userName = entry.getName();
            String userAppPath = entry.getAppPath();
            long inodeQuota = entry.getInodeQuota();
            double inodeAlarmThreshold = entry.getInodeAlarmThreshold();

            PreparedStatement pstat = dbConn.prepareStatement(
                    "SELECT * FROM " + extUserINodeQuotaInfoTableName + " " +
                            "WHERE cluster_name=? " +
                            "AND user_name=? " +
                            "AND user_app_path=? " +
                            "AND inode_is_sub_path=0 " +
                            "ORDER BY detection_timestamp DESC " +
                            "LIMIT 1");
            pstat.setString(1, clusterName.toUpperCase());
            pstat.setString(2, userName);
            pstat.setString(3, userAppPath);
            ResultSet res = pstat.executeQuery();

            if (res.next()) {

                Date detectionTimestamp =
                        new Date(res.getTimestamp("detection_timestamp").getTime());
                long inodeFileCount = res.getLong("inode_file_count");
                long inodeDirectoryCount = res.getLong("inode_directory_count");

                monitorInfo.add(new UserINodeQuotaInfo(
                        userName,
                        userAppPath,
                        inodeQuota,
                        inodeFileCount,
                        inodeDirectoryCount,
                        detectionTimestamp,
                        inodeAlarmThreshold));

            } else {
                monitorInfo.add(new UserINodeQuotaInfo(
                        userName,
                        userAppPath,
                        inodeQuota,
                        -1,
                        -1,
                        new Date(0),
                        inodeAlarmThreshold));
            }

            pstat.close();
        }

        dbConn.close();

        return monitorInfo;
    }     */

    /*
    public synchronized NavigableMap<Date, UserINodeQuotaInfo> getNDaysUserINodeQuotaInfoOfTargetPath(
            String clusterName, String userName, String userAppPath, long lastDetectedAt, int days)
            throws SQLException {

        NavigableMap<Date, UserINodeQuotaInfo> monitorInfo =
                new TreeMap<Date, UserINodeQuotaInfo>();

        Configuration hmaConf = new Configuration(true, "hma");
        String confBaseDirPath = hmaConf.get(
                "hma.conf.base.dir", "/home/monitor/hma-v1.1/hma/conf/");
        if (!confBaseDirPath.endsWith("/")) {
            confBaseDirPath = confBaseDirPath + "/";
        }
        String defaultConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-default.xml";
        String siteConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-site.xml";
        File defaultConfFile = new File(defaultConfFilePath);
        File siteConfFile = new File(siteConfFilePath);
        if (!defaultConfFile.exists() && !siteConfFile.exists()) {
            throw new RuntimeException();
        }

        UserQuotaConfiguration quotaConf =
                new UserQuotaConfiguration(false);
        if (defaultConfFile.exists()) {
            quotaConf.addConfigurationResource(defaultConfFile);
        }
        if (siteConfFile.exists()) {
            quotaConf.addConfigurationResource(siteConfFile);
        }
        NavigableSet<UserQuotaConfigurationEntry> confEntries = quotaConf.loadEntries();

        UserQuotaConfigurationEntry targetConfEntry = null;
        for (UserQuotaConfigurationEntry confEntry : confEntries) {
            if (confEntry.getName().equals(userName) &&
                    confEntry.getAppPath().equals(userAppPath)) {
                targetConfEntry = confEntry;
                break;
            }
        }

        long inodeQuota = targetConfEntry.getInodeQuota();
        double alarmThreshold = targetConfEntry.getInodeAlarmThreshold();

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT * FROM " + extUserINodeQuotaInfoTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND user_name=? " +
                        "AND user_app_path=? " +
                        "AND detection_timestamp<=? " +
                        "AND detection_timestamp>? " +
                        "AND inode_is_sub_path=0 " +
                        "ORDER BY detection_timestamp");
        pstat.setString(1, clusterName.toUpperCase());
        pstat.setString(2, userName);
        pstat.setString(3, userAppPath);
        pstat.setString(4, dateFormat.format(new Date(lastDetectedAt)));
        pstat.setString(5,
                dateFormat.format(new Date(lastDetectedAt - ((long) days) * 24 * 3600 * 1000)));

        ResultSet res = pstat.executeQuery();
        while (res.next()) {

            Date detectionTimestamp =
                    new Date(res.getTimestamp("detection_timestamp").getTime());
            String hdfsPath = res.getString("user_app_path");
            long inodeFileCount = res.getLong("inode_file_count");
            long inodeDirectoryCount = res.getLong("inode_directory_count");

            monitorInfo.put(detectionTimestamp,
                    new UserINodeQuotaInfo(
                            userName,
                            hdfsPath,
                            inodeQuota,
                            inodeFileCount,
                            inodeDirectoryCount,
                            detectionTimestamp,
                            alarmThreshold));
        }

        pstat.close();
        dbConn.close();

        return monitorInfo;
    }

    public synchronized NavigableSet<UserINodeQuotaInfo> getUserINodeQuotaTotalInfoOfTargetPath(
            String clusterName, String userName, String userAppPath, long detectedAt)
            throws SQLException {

        NavigableSet<UserINodeQuotaInfo> monitorInfo =
                new TreeSet<UserINodeQuotaInfo>(new UserINodeQuotaInfoComparator());

        Configuration hmaConf = new Configuration(true, "hma");
        String confBaseDirPath = hmaConf.get(
                "hma.conf.base.dir", "/home/monitor/hma-v1.1/hma/conf/");
        if (!confBaseDirPath.endsWith("/")) {
            confBaseDirPath = confBaseDirPath + "/";
        }
        String defaultConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-default.xml";
        String siteConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-site.xml";
        File defaultConfFile = new File(defaultConfFilePath);
        File siteConfFile = new File(siteConfFilePath);
        if (!defaultConfFile.exists() && !siteConfFile.exists()) {
            throw new RuntimeException();
        }

        UserQuotaConfiguration quotaConf =
                new UserQuotaConfiguration(false);
        if (defaultConfFile.exists()) {
            quotaConf.addConfigurationResource(defaultConfFile);
        }
        if (siteConfFile.exists()) {
            quotaConf.addConfigurationResource(siteConfFile);
        }
        NavigableSet<UserQuotaConfigurationEntry> confEntries = quotaConf.loadEntries();

        UserQuotaConfigurationEntry targetConfEntry = null;
        for (UserQuotaConfigurationEntry confEntry : confEntries) {
            if (confEntry.getName().equals(userName) &&
                    confEntry.getAppPath().equals(userAppPath)) {
                targetConfEntry = confEntry;
                break;
            }
        }

        long inodeQuota = targetConfEntry.getInodeQuota();
        double alarmThreshold = targetConfEntry.getInodeAlarmThreshold();

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT * FROM " + extUserINodeQuotaInfoTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND user_name=? " +
                        "AND detection_timestamp=? ");
        pstat.setString(1, clusterName.toUpperCase());
        pstat.setString(2, userName);
        pstat.setString(3, dateFormat.format(new Date(detectedAt)));

        ResultSet res = pstat.executeQuery();
        while (res.next()) {

            Date detectionTimestamp =
                    new Date(res.getTimestamp("detection_timestamp").getTime());
            String hdfsPath = res.getString("user_app_path");
            long inodeFileCount = res.getLong("inode_file_count");
            long inodeDirectoryCount = res.getLong("inode_directory_count");

            monitorInfo.add(new UserINodeQuotaInfo(
                    userName,
                    hdfsPath,
                    inodeQuota,
                    inodeFileCount,
                    inodeDirectoryCount,
                    detectionTimestamp,
                    alarmThreshold));
        }

        pstat.close();
        dbConn.close();

        return monitorInfo;

    }


    public static class UserSpaceQuotaInfo {

        private String userName = null;

        private String userAppPath = null;

        private long spaceQuota = -1L;

        private long spaceConsumed = 0L;

        private Date detectionTimestamp = null;

        private double spaceAlarmThreshold = 80.0;

        public UserSpaceQuotaInfo(
                String userName,
                String userAppPath,
                long spaceQuota,
                long spaceConsumed,
                Date detectionTimestamp,
                double spaceAlarmThreshold) {
            this.userName = userName;
            this.userAppPath = userAppPath;
            this.spaceQuota = spaceQuota;
            this.spaceConsumed = spaceConsumed;
            this.detectionTimestamp = detectionTimestamp;
            this.spaceAlarmThreshold = spaceAlarmThreshold;
        }

        public String getUserName() {
            return userName;
        }

        public String getUserAppPath() {
            return userAppPath;
        }

        public long getSpaceQuota() {
            return spaceQuota;
        }

        public long getSpaceConsumed() {
            return spaceConsumed;
        }

        public Date getDetectionTimestamp() {
            return detectionTimestamp;
        }

        public double getSpaceAlarmThreshold() {
            return spaceAlarmThreshold;
        }

    }

    public static class UserSpaceQuotaInfoComparator
            implements Comparator<UserSpaceQuotaInfo> {

        @Override
        public int compare(UserSpaceQuotaInfo info0, UserSpaceQuotaInfo info1) {

            String userName0 = info0.getUserName();
            String userName1 = info1.getUserName();

            if (userName0.compareTo(userName1) != 0)
                return userName0.compareTo(userName1);

            String userAppPath0 = info0.getUserAppPath();
            String userAppPath1 = info1.getUserAppPath();

            return userAppPath0.compareTo(userAppPath1);
        }

    }

    public synchronized NavigableSet<UserSpaceQuotaInfo>
    getAllUserSpaceQuotaInfo(String clusterName) throws SQLException {

        NavigableSet<UserSpaceQuotaInfo> monitorInfo =
                new TreeSet<UserSpaceQuotaInfo>(new UserSpaceQuotaInfoComparator());

        Configuration hmaConf = new Configuration(true, "hma");
        String confBaseDirPath = hmaConf.get(
                "hma.conf.base.dir", "/home/monitor/hma-v1.1/hma/conf/");
        if (!confBaseDirPath.endsWith("/")) {
            confBaseDirPath = confBaseDirPath + "/";
        }
        String defaultConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-default.xml";
        String siteConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-site.xml";
        File defaultConfFile = new File(defaultConfFilePath);
        File siteConfFile = new File(siteConfFilePath);
        if (!defaultConfFile.exists() && !siteConfFile.exists()) {
            throw new RuntimeException();
        }

        UserQuotaConfiguration quotaConf =
                new UserQuotaConfiguration(false);
        if (defaultConfFile.exists()) {
            quotaConf.addConfigurationResource(defaultConfFile);
        }
        if (siteConfFile.exists()) {
            quotaConf.addConfigurationResource(siteConfFile);
        }
        NavigableSet<UserQuotaConfigurationEntry> confEntries =
                quotaConf.loadEntries();

        Connection dbConn = createDBConnection();
        for (UserQuotaConfigurationEntry entry : confEntries) {

            System.out.println(entry);

            String userName = entry.getName();
            String userAppPath = entry.getAppPath();
            long spaceQuota = entry.getSpaceQuota();
            double spaceAlarmThreshold = entry.getSpaceAlarmThreshold();

            PreparedStatement pstat = dbConn.prepareStatement(
                    "SELECT * FROM " + extUserSpaceQuotaInfoTableName + " " +
                            "WHERE cluster_name=? " +
                            "AND user_name=? " +
                            "AND user_app_path=? " +
                            "AND inode_is_sub_path=0 " +
                            "ORDER BY detection_timestamp DESC " +
                            "LIMIT 1");
            pstat.setString(1, clusterName.toUpperCase());
            pstat.setString(2, userName);
            pstat.setString(3, userAppPath);
            ResultSet res = pstat.executeQuery();

            if (res.next()) {

                Date detectionTimestamp =
                        new Date(res.getTimestamp("detection_timestamp").getTime());
                long spaceConsumed = res.getLong("space_consumed");

                monitorInfo.add(new UserSpaceQuotaInfo(
                        userName,
                        userAppPath,
                        spaceQuota,
                        spaceConsumed,
                        detectionTimestamp,
                        spaceAlarmThreshold));

            } else {
                monitorInfo.add(new UserSpaceQuotaInfo(
                        userName,
                        userAppPath,
                        spaceQuota,
                        -1,
                        new Date(0),
                        spaceAlarmThreshold));
            }

            pstat.close();
        }

        dbConn.close();

        return monitorInfo;
    }

    public synchronized NavigableMap<Date, UserSpaceQuotaInfo> getNDaysUserSpaceQuotaInfoOfTargetPath(
            String clusterName, String userName, String userAppPath, long lastDetectedAt, int days)
            throws SQLException {

        NavigableMap<Date, UserSpaceQuotaInfo> monitorInfo =
                new TreeMap<Date, UserSpaceQuotaInfo>();

        Configuration hmaConf = new Configuration(true, "hma");
        String confBaseDirPath = hmaConf.get(
                "hma.conf.base.dir", "/home/monitor/hma-v1.1/hma/conf/");
        if (!confBaseDirPath.endsWith("/")) {
            confBaseDirPath = confBaseDirPath + "/";
        }
        String defaultConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-default.xml";
        String siteConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-site.xml";
        File defaultConfFile = new File(defaultConfFilePath);
        File siteConfFile = new File(siteConfFilePath);
        if (!defaultConfFile.exists() && !siteConfFile.exists()) {
            throw new RuntimeException();
        }

        UserQuotaConfiguration quotaConf =
                new UserQuotaConfiguration(false);
        if (defaultConfFile.exists()) {
            quotaConf.addConfigurationResource(defaultConfFile);
        }
        if (siteConfFile.exists()) {
            quotaConf.addConfigurationResource(siteConfFile);
        }
        NavigableSet<UserQuotaConfigurationEntry> confEntries = quotaConf.loadEntries();

        UserQuotaConfigurationEntry targetConfEntry = null;
        for (UserQuotaConfigurationEntry confEntry : confEntries) {
            if (confEntry.getName().equals(userName) &&
                    confEntry.getAppPath().equals(userAppPath)) {
                targetConfEntry = confEntry;
                break;
            }
        }

        long spaceQuota = targetConfEntry.getSpaceQuota();
        double alarmThreshold = targetConfEntry.getSpaceAlarmThreshold();

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT * FROM " + extUserSpaceQuotaInfoTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND user_name=? " +
                        "AND user_app_path=? " +
                        "AND detection_timestamp<=? " +
                        "AND detection_timestamp>? " +
                        "AND inode_is_sub_path=0 " +
                        "ORDER BY detection_timestamp");
        pstat.setString(1, clusterName.toUpperCase());
        pstat.setString(2, userName);
        pstat.setString(3, userAppPath);
        pstat.setString(4, dateFormat.format(new Date(lastDetectedAt)));
        pstat.setString(5,
                dateFormat.format(new Date(lastDetectedAt - ((long) days) * 24 * 3600 * 1000)));

        ResultSet res = pstat.executeQuery();
        while (res.next()) {

            Date detectionTimestamp =
                    new Date(res.getTimestamp("detection_timestamp").getTime());
            String hdfsPath = res.getString("user_app_path");
            long spaceConsumed = res.getLong("space_consumed");

            monitorInfo.put(detectionTimestamp,
                    new UserSpaceQuotaInfo(
                            userName,
                            hdfsPath,
                            spaceQuota,
                            spaceConsumed,
                            detectionTimestamp,
                            alarmThreshold));
        }

        pstat.close();
        dbConn.close();

        return monitorInfo;
    }

    public synchronized NavigableSet<UserSpaceQuotaInfo> getUserSpaceQuotaTotalInfoOfTargetPath(
            String clusterName, String userName, String userAppPath, long detectedAt)
            throws SQLException {

        NavigableSet<UserSpaceQuotaInfo> monitorInfo =
                new TreeSet<UserSpaceQuotaInfo>(new UserSpaceQuotaInfoComparator());

        Configuration hmaConf = new Configuration(true, "hma");
        String confBaseDirPath = hmaConf.get(
                "hma.conf.base.dir", "/home/monitor/hma-v1.1/hma/conf/");
        if (!confBaseDirPath.endsWith("/")) {
            confBaseDirPath = confBaseDirPath + "/";
        }
        String defaultConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-default.xml";
        String siteConfFilePath =
                confBaseDirPath + clusterName.toUpperCase() +
                        "/hma-user-quota-site.xml";
        File defaultConfFile = new File(defaultConfFilePath);
        File siteConfFile = new File(siteConfFilePath);
        if (!defaultConfFile.exists() && !siteConfFile.exists()) {
            throw new RuntimeException();
        }

        UserQuotaConfiguration quotaConf =
                new UserQuotaConfiguration(false);
        if (defaultConfFile.exists()) {
            quotaConf.addConfigurationResource(defaultConfFile);
        }
        if (siteConfFile.exists()) {
            quotaConf.addConfigurationResource(siteConfFile);
        }
        NavigableSet<UserQuotaConfigurationEntry> confEntries = quotaConf.loadEntries();

        UserQuotaConfigurationEntry targetConfEntry = null;
        for (UserQuotaConfigurationEntry confEntry : confEntries) {
            if (confEntry.getName().equals(userName) &&
                    confEntry.getAppPath().equals(userAppPath)) {
                targetConfEntry = confEntry;
                break;
            }
        }

        long spaceQuota = targetConfEntry.getSpaceQuota();
        double alarmThreshold = targetConfEntry.getSpaceAlarmThreshold();

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT * FROM " + extUserSpaceQuotaInfoTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND user_name=? " +
                        "AND detection_timestamp=? ");
        pstat.setString(1, clusterName.toUpperCase());
        pstat.setString(2, userName);
        pstat.setString(3, dateFormat.format(new Date(detectedAt)));

        ResultSet res = pstat.executeQuery();
        while (res.next()) {

            Date detectionTimestamp =
                    new Date(res.getTimestamp("detection_timestamp").getTime());
            String hdfsPath = res.getString("user_app_path");
            long spaceConsumed = res.getLong("space_consumed");

            monitorInfo.add(new UserSpaceQuotaInfo(
                    userName,
                    hdfsPath,
                    spaceQuota,
                    spaceConsumed,
                    detectionTimestamp,
                    alarmThreshold));
        }

        pstat.close();
        dbConn.close();

        return monitorInfo;

    }    */


    /*
     * The following fields & methods are for `collectionmetrics.jsp'
     */
    public synchronized SortedMap<String, List<CollectionMetricRecord>>
    getAllItemCollectionMetrics(String clusterName, long beg, long end) throws SQLException {

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT item_name, detection_timestamp, time_consumed " +
                        "FROM " + collectionMetricTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND detection_timestamp>? " +
                        "AND detection_timestamp<=?");
        pstat.setString(1, clusterName);
        pstat.setString(2, dateFormat.format(new Date(beg)));
        pstat.setString(3, dateFormat.format(new Date(end)));
        ResultSet res = pstat.executeQuery();

        SortedMap<String, List<CollectionMetricRecord>> metrics =
                new TreeMap<String, List<CollectionMetricRecord>>();

        while (res.next()) {
            String itemName = res.getString("item_name");
            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            Long timeConsumed = res.getLong("time_consumed");
            CollectionMetricRecord record =
                    new CollectionMetricRecord(
                            clusterName,
                            itemName,
                            detectionTimestamp.getTime(),
                            timeConsumed);
            List<CollectionMetricRecord> list = metrics.get(itemName);
            if (list == null) {
                list = new ArrayList<CollectionMetricRecord>();
                metrics.put(itemName, list);
            }
            list.add(record);
        }

        pstat.close();
        dbConn.close();

        return metrics;
    }

    public synchronized List<CollectionMetricRecord> getCollectionMetrics(
            String clusterName, String itemName, long beg, long end) throws SQLException {

        Connection dbConn = createDBConnection();
        PreparedStatement pstat = dbConn.prepareStatement(
                "SELECT detection_timestamp, time_consumed " +
                        "FROM " + collectionMetricTableName + " " +
                        "WHERE cluster_name=? " +
                        "AND item_name=? " +
                        "AND detection_timestamp>? " +
                        "AND detection_timestamp<=?");
        pstat.setString(1, clusterName);
        pstat.setString(2, itemName);
        pstat.setString(3, dateFormat.format(new Date(beg)));
        pstat.setString(4, dateFormat.format(new Date(end)));
        ResultSet res = pstat.executeQuery();

        List<CollectionMetricRecord> records =
                new ArrayList<CollectionMetricRecord>();

        while (res.next()) {
            Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
            Long timeConsumed = res.getLong("time_consumed");
            CollectionMetricRecord record =
                    new CollectionMetricRecord(
                            clusterName,
                            itemName,
                            detectionTimestamp.getTime(),
                            timeConsumed);
            records.add(record);
        }

        pstat.close();
        dbConn.close();

        return records;
    }

    /**
     * For daily inspection function by weizhonghui@baidu.com
     */
    public synchronized Map<String, String> checkInspectionResult(
            String clusterName,
            Map<String, String> inspectEntry, String begStr, String endStr) throws SQLException {

        Map<String, String> checkResult = new HashMap<String, String>();

        Connection dbConn = createDBConnection();


        Iterator<Map.Entry<String, String>> iter = inspectEntry.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String strategyName = entry.getKey();


            PreparedStatement pstat = null;

            pstat = dbConn.prepareStatement(
                    "SELECT detection_timestamp, info_level, info_content " +
                            "FROM " + inspectionTable + " " +
                            "WHERE cluster_name=? " +
                            "AND strategy_name=? " +
                            "AND detection_timestamp>? " +
                            "AND detection_timestamp<? " +
                            "ORDER BY detection_timestamp DESC limit 1");
            pstat.setString(1, clusterName);
            pstat.setString(2, strategyName);
            pstat.setString(3, begStr);
            pstat.setString(4, endStr);
            ResultSet res = pstat.executeQuery();

            if (res.next()) {
                Timestamp detectionTimestamp = res.getTimestamp("detection_timestamp");
                String strategyLevel = res.getString("info_level");
                String inspectRes = res.getString("info_content");
                //System.out.println(detectionTimestamp + " " );

                checkResult.put(strategyName, inspectRes);

            } else {
                checkResult.put(strategyName, "NODATA");
            }


        }

        return checkResult;
    }

}

