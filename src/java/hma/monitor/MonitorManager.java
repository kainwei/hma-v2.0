/**
 *
 */
package hma.monitor;

import hma.conf.Configuration;
import hma.conf.Configured;
import hma.util.LOG;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author guoyezhi
 */
public class MonitorManager extends Configured implements HMAMonitorProtocol {

    private static MonitorManager  monitorManager = null;

    private static boolean isUpdate = false;

    private Server server;

    private InetSocketAddress address;

    private static boolean isAlarm = true;

    private final long alarmInterval = 60 * 1000 * 60 * 2; // 2 hours

    private MonitorRecover recover = new MonitorRecover();

    private MonitorManager(Configuration mmConf) {
        super(mmConf);
        MonitorManager.monitorManager = this;
        startMonitorManager();
    }

    private void startMonitorManager() {
        MonitorItemManager miManager =
                new MonitorItemManager();
        MonitorStrategyManager msManager =
                new MonitorStrategyManager(miManager.getExtractedDataPool());
        new Thread(miManager).start();

        /*try {
            createManagerRPC();
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        recover.start();

        try {
            TimeUnit.SECONDS.sleep(120);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        new Thread(msManager).start();


    }

    public static MonitorManager createMonitorManager(
            String args[],
            Configuration mmConf) {

        // TODO: utilize args[] in future
        System.out.println("Args size : " + args.length);
        if (args.length == 2) {
            if (args[1].equals("updateamm")) {
                System.out.println("Update appmaster monitor ...");
                isUpdate = true;
            }
        }

        if (MonitorManager.monitorManager == null) {
            if (mmConf == null)
                mmConf = new Configuration(true, "hma");
            return new MonitorManager(mmConf);
        } else {
            //LOG.warn("MonitorManager has already been created.");
            return MonitorManager.monitorManager;
        }
    }

    public static MonitorManager getMonitorManager() {
        return MonitorManager.monitorManager;
    }

    /**
     * @return the clusterName
     */
    public static String getMonitoredClusterName() {
        MonitorManager manager =
                MonitorManager.getMonitorManager();
        if (manager == null)
            return null;
        return manager.getConf().get(
                "monitored.hadoop.cluster.id");
    }

    public static Configuration getGlobalConf() {
        if (monitorManager == null)
            return null;
        return monitorManager.getConf();
    }

    public static void startupShutDownMessage() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down");
                LOG.info("Shutting down");
                System.out.flush();
                System.err.flush();

            }
        });
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        startupShutDownMessage();
        //System.out.println(MonitorManager.class.getResource("MonitorItem.java"));
        MonitorManager monitorManager =
                MonitorManager.createMonitorManager(args, null);
        System.out.println(MonitorManager.getMonitoredClusterName());
    }

    /*private void createManagerRPC() throws IOException {
        address = NetUtils.createSocketAddr("127.0.0.1", 0);
        server = RPC.getServer(this, address.getHostName(), address.getPort(), 3, false, new org.apache.hadoop.conf.Configuration());
        server.start();
        LOG.info("HMA manager rpc server is started at : " + server.getListenerAddress().getAddress() + ":" + server.getListenerAddress().getPort());
        saveRPCInfo();
        LOG.info("HMA manager rpc server info is saved ");
    }*/

    private void saveRPCInfo() {
        File f = new File(System.getProperty("hma.home.dir") + "/conf/" + getMonitoredClusterName().toUpperCase() + "/hma-rpc.prop");
        if (f.exists() && f.isFile()) {
            System.out.println(" ^^^ hma-rpc.prop has been deleted ...");
            f.delete();
        }
        try {
            f.createNewFile();
            BufferedWriter output = new BufferedWriter(new FileWriter(f));
            output.write(getMonitoredClusterName() + ":" + server.getListenerAddress().getAddress().getHostName() + ":" + server.getListenerAddress().getPort() + "\n");
            output.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public static boolean isAlarm() {
        return isAlarm;
    }

    @Override
    public boolean suspendAlarm(String cluster) {
        if (getMonitoredClusterName().equals(cluster.toUpperCase())) {
            isAlarm = false;
            recover.resetTime();
            recover.isRunning = true;

            LOG.warn("Receive suspendAlarm request");
            String message = cluster + "集群短信报警已被关闭，2小时后自动恢复";
            sendSMS(message);
            return true;
        }
        LOG.warn("Receive suspendAlarm request, cluster is not match");
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean recoverAlarm(String cluster) {
        if (getMonitoredClusterName().equals(cluster.toUpperCase())) {
            isAlarm = true;
            recover.isRunning = false;

            LOG.warn("Receive recoverAlarm request");
            String message = cluster + "集群短信报警已经恢复，请务必Check服务状态";
            sendSMS(message);
            return true;
        }
        LOG.warn("Receive recoverAlarm request, cluster is not match");
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MonitorStatus getAlarmStatus(String cluster) {
        if (getMonitoredClusterName().equals(cluster.toUpperCase())) {
            MonitorStatus s = new MonitorStatus();
            s.setAlarm(isAlarm);
            s.setBegin(recover.begin);
            s.setEnd(recover.end);
            return s;
        }
        LOG.warn("Receive getAlarmStatus request, cluster is not match");
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }

    //@Override
    public boolean isAuthorized(String s) throws IOException {
        return true;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void sendSMS(String message) {
        String[] servers =
                MonitorManager.getGlobalConf().getStrings("baidu.gsm.servers");
        String hmaGSMsend =
                System.getProperty("hma.home.dir") + "/conf/" +
                        MonitorManager.getMonitoredClusterName() + "/gsmsend.hma";
        String[] receivers =
                MonitorManager.getGlobalConf().getStrings("sre.mobile.reporter.critical.level.alarm.receivers");
        //String[] receivers = {"18612540350"};

        Runtime rt = Runtime.getRuntime();

        for (int i = 0; i < receivers.length; i++) {

            String[] gsmsendComm = new String[3 + 2 * servers.length];
            gsmsendComm[0] = "gsmsend";
            for (int j = 0; j < servers.length; j++) {
                gsmsendComm[1 + 2 * j] = "-s";
                gsmsendComm[2 + 2 * j] = servers[j].trim();
            }
            gsmsendComm[1 + 2 * servers.length] = receivers[i].trim();
            gsmsendComm[2 + 2 * servers.length] = message;

            for (String str : gsmsendComm) {
                System.out.print(str + " ");
            }
            System.out.println();

            gsmsendComm[0] = hmaGSMsend;
            try {
                rt.exec(gsmsendComm);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }
    }


    class MonitorRecover extends Thread {

        private long begin;
        private long end;
        public boolean isRunning = false;

        public MonitorRecover() {
            begin = System.currentTimeMillis();
            end = begin + alarmInterval;
        }

        public void resetTime() {
            begin = System.currentTimeMillis();
            end = begin + alarmInterval;
        }

        @Override
        public void run() {
            while (true) {
                if (isRunning) {
                    if (isAlarm == true)
                        continue;
                    long now = System.currentTimeMillis();
                    while (now < end && isRunning) {
                        try {
                            TimeUnit.SECONDS.sleep(120);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        now = System.currentTimeMillis();
                    }
                    if (isRunning) {
                        isAlarm = true;
                        LOG.warn("Monitor is recovered ...");
                        String message = getMonitoredClusterName() + "集群短信报警已经关闭2小时，自动恢复";
                        sendSMS(message);
                    }
                } else {
                    try {
                        TimeUnit.SECONDS.sleep(120);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
