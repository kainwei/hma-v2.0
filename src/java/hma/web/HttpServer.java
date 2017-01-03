/**
 *
 */
package hma.web;

import hma.conf.Configuration;
import hma.monitor.HMAMonitorProtocol;
import hma.util.HmaStatusJsonIf;
import hma.util.LOG;
import hma.web.oob.OOBMonitorImportDBDumper;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

import javax.servlet.http.HttpServlet;
import java.io.*;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


/**
 * @author guoyezhi
 */
public class HttpServer {

    protected final Server webServer;

    protected final Connector listener;

    protected final boolean findPort;

    protected final Map<Context, Boolean> defaultContexts =
            new HashMap<Context, Boolean>();

    private static final int MAX_RETRIES = 10;

    private WebAppContext webapp;

    private static Map<String, HMAMonitorProtocol> monitorControl = new HashMap<String, HMAMonitorProtocol>();


    /**
     * @throws IOException
     */
    public HttpServer(String name, String bindAddress, int port,
                      boolean findPort) throws IOException {
        this(name, bindAddress, port, findPort,
                new Configuration(true, "hma"));
    }

    public HttpServer(String name, String bindAddress, int port,
                      boolean findPort, Configuration conf) throws IOException {

        this.webServer = new Server();
        this.findPort = findPort;

        /** add Connector */
        this.listener = createBaseListener(conf);
        if (conf == null) {
            this.listener.setHost(name);
            this.listener.setPort(port);
        } else {
            System.out.println(conf.get("web.http.address.name"));
            this.listener.setHost(
                    conf.get("web.http.address.name", "hma.dmop.baidu.com"));
            System.out.println(conf.get("web.http.address.port"));
            this.listener.setPort(conf.getInt("web.http.address.port", 80));
        }
        this.webServer.addConnector(listener);

        /** set thread pool */
        this.webServer.setThreadPool(new QueuedThreadPool());

        /** set handler */
        webapp = new WebAppContext();
        webapp.setResourceBase(conf.get("web.http.app.resource.base"));
        webapp.setContextPath("/");
        webServer.setHandler(webapp);

		/*
		final String appDir = getWebAppsPath();
		webAppContext = new WebAppContext();
		webAppContext.setContextPath("/hma");
		//webAppContext.setWar(appDir + "/" + name);
		webAppContext.setWar("D:\\eclipse\\guoyezhi\\workspace\\HMA\\webapps\\hma");
		webAppContext.setLogUrlOnStart(false);
		webServer.addHandler(new ResourceHandler());
		webServer.setHandler(webAppContext);
		System.out.println(webAppContext.getResourceBase());
		
		
		ContextHandler context = new ContextHandler();
        context.setContextPath("/hma");
        context.setResourceBase("D:\\eclipse\\guoyezhi\\workspace\\HMA\\webapps");
        context.setClassLoader(Thread.currentThread().getContextClassLoader());
        context.setHandler(new ResourceHandler());
        webServer.setHandler(context);
        */
		
		/*
		ResourceHandler publicDocs = new ResourceHandler();
		publicDocs.setResourceBase("D:\\eclipse\\guoyezhi\\workspace\\HMA\\webapps\\hma");
		publicDocs.setWelcomeFiles(new String[]{"index.htm"});
		webServer.setHandler(publicDocs);
		*/

        //initRpcClient();
    }

    public static Map<String, HMAMonitorProtocol> getRpcClient() {
        initRpcClient();
        return monitorControl;
    }

    protected Connector createBaseListener(Configuration conf)
            throws IOException {
        SelectChannelConnector ret = new SelectChannelConnector();
        ret.setLowResourceMaxIdleTime(10000);
        ret.setAcceptQueueSize(128);
        ret.setResolveNames(false);
        ret.setUseDirectBuffers(false);
        return ret;
    }

    /**
     * Get the pathname to the webapps files.
     *
     * @return the pathname as a URL
     * @throws IOException if 'webapps' directory cannot be found on CLASSPATH.
     */
    public String getWebAppsPath() throws IOException {
        URL url = getClass().getClassLoader().getResource("webapps");
        if (url == null)
            throw new IOException("webapps not found in CLASSPATH");
        return url.toString();
    }

    /**
     * Start the server. Does not wait for the server to start.
     */
    public void start() throws IOException {
        try {
            int port = 0;
            int oriPort = listener.getPort(); // The original requested port
            while (true) {
                try {
                    port = webServer.getConnectors()[0].getLocalPort();
                    LOG.info("Port returned by webServer.getConnectors()[0]."
                            + "getLocalPort() before open() is " + port
                            + ". Opening the listener on " + oriPort);
                    listener.open();
                    port = listener.getLocalPort();
                    LOG.info("listener.getLocalPort() returned "
                            + listener.getLocalPort()
                            + " webServer.getConnectors()[0].getLocalPort() returned "
                            + webServer.getConnectors()[0].getLocalPort());
                    // Workaround to handle the problem reported in HADOOP-4744
                    if (port < 0) {
                        Thread.sleep(100);
                        int numRetries = 1;
                        while (port < 0) {
                            LOG.warn("listener.getLocalPort returned " + port);
                            if (numRetries++ > MAX_RETRIES) {
                                throw new Exception(
                                        " listener.getLocalPort is returning "
                                                + "less than 0 even after "
                                                + numRetries + " resets");
                            }
                            for (int i = 0; i < 2; i++) {
                                LOG.info("Retrying listener.getLocalPort()");
                                port = listener.getLocalPort();
                                if (port > 0) {
                                    break;
                                }
                                Thread.sleep(200);
                            }
                            if (port > 0) {
                                break;
                            }
                            LOG.info("Bouncing the listener");
                            listener.close();
                            Thread.sleep(1000);
                            listener.setPort(oriPort == 0 ? 0 : (oriPort += 1));
                            listener.open();
                            Thread.sleep(100);
                            port = listener.getLocalPort();
                        }
                    } // Workaround end
                    LOG.info("Jetty bound to port " + port);
                    webServer.start();

                    break;
                } catch (IOException ex) {
                    // if this is a bind exception,
                    // then try the next port number.
                    if (ex instanceof BindException) {
                        if (!findPort) {
                            throw (BindException) ex;
                        }
                    } else {
                        LOG.info("HttpServer.start() threw a non Bind IOException");
                        throw ex;
                    }
                } catch (MultiException ex) {
                    LOG.info("HttpServer.start() threw a MultiException");
                    throw ex;
                }
                listener.setPort((oriPort += 1));
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Problem starting http server", e);
        }
    }

    /**
     * stop the server
     */
    public void stop() throws Exception {
        listener.close();
        webServer.stop();
    }

    public void join() throws InterruptedException {
        webServer.join();
    }

    public void addServlet(String name, String pathSpec,
                           Class<? extends HttpServlet> servletClass) {
        ServletHolder holder = new ServletHolder(servletClass);
        if (name != null) {
            holder.setName(name);
        }
        webapp.addServlet(holder, pathSpec);
    }

    private static void initRpcClient() {
        File parent = new File(System.getProperty("hma.home.dir") + "/conf/");
        LOG.info("Begin to initRpcClient : " + System.getProperty("hma.home.dir") + "/conf/");
        for (File current : parent.listFiles()) {
            if (current.isDirectory()) {
                for (File child : current.listFiles()) {
                    if (child.isFile() && child.getName().equals("hma-rpc.prop")) {
                        LOG.info("Found hma-rpc.prop at " + child.getAbsoluteFile());
                        try {
                            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(child)));
                            String data;
                            String cluster;
                            String host;
                            int port;
                            while ((data = br.readLine()) != null) {
                                if (data.split(":").length == 3) {
                                    cluster = data.split(":")[0];
                                    host = data.split(":")[1];
                                    port = Integer.parseInt(data.split(":")[2]);

                                    InetSocketAddress addr;
                                    addr = NetUtils.createSocketAddr(host, port);
                                    HMAMonitorProtocol p = (HMAMonitorProtocol) RPC.waitForProxy(HMAMonitorProtocol.class, 0, addr, new org.apache.hadoop.conf.Configuration());
                                    if (monitorControl.containsKey(cluster)) {
                                        RPC.stopProxy(monitorControl.get(cluster));
                                    }
                                    monitorControl.put(cluster, p);
                                    LOG.info("hma-rpc.prop loaded for " + cluster);
                                }
                            }
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        } catch (IOException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                }
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        System.out.println("###Servlet has been added for the first time ");

        int tmpInfoPort = 8090;
        String infoHost = "127.0.0.1";
        Configuration hmaConf = new Configuration(true, "hma");

        HttpServer infoServer = new HttpServer(
                "hma-master-host",
                infoHost,
                tmpInfoPort,
                tmpInfoPort == 0,
                hmaConf);

        infoServer.start();
        infoServer.addServlet("hmastatus", "/hmastatus", HmaStatusJsonIf.class);
        infoServer.addServlet("manager", "/manage", HmaMonitorManager.class);
        infoServer.addServlet("inspection", "/inspection", HmaInspection.class);
        System.out.println("###Servlet has been added ");
        System.out.print("Hello");


        new Thread(new OOBMonitorImportDBDumper(hmaConf)).start();

    }


}

