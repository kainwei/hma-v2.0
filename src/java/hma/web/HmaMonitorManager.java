package hma.web;

import hma.monitor.HMAMonitorProtocol;
import hma.monitor.MonitorStatus;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jiangtao02
 * Date: 13-4-28
 * Time: 下午4:11
 * To change this template use File | Settings | File Templates.
 */
public class HmaMonitorManager extends HttpServlet {

    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {

        String cmd = req.getParameter("cmd");
        String CLUSTER = req.getParameter("cluster");
        PrintWriter out = res.getWriter();
        SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

        if(cmd.equals("get")){
            JSONArray array = new JSONArray();
            Map<String, HMAMonitorProtocol> map = HttpServer.getRpcClient();
            Iterator<Map.Entry<String, HMAMonitorProtocol>> iter = map.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry<String, HMAMonitorProtocol> entry = iter.next();
                String cluster = entry.getKey();
                HMAMonitorProtocol p = entry.getValue();
                MonitorStatus status = p.getAlarmStatus(cluster);

                JSONObject j = new JSONObject();
                try {
                    j.put("cluster", cluster);
                    j.put("begin", format.format(status.getBegin()));
                    j.put("end", format.format(status.getEnd()));
                    j.put("remain", (status.getEnd() - System.currentTimeMillis())/1000/60);
                    j.put("isAlarm", status.getAlarm());
                    array.put(j);
                } catch (JSONException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

            }
            out.write(array.toString());
        } else if(cmd.equals("stop")) {
            Map<String, HMAMonitorProtocol> map = HttpServer.getRpcClient();
            HMAMonitorProtocol p = map.get(CLUSTER);
            p.suspendAlarm(CLUSTER);
        } else if(cmd.equals("recover")) {
            Map<String, HMAMonitorProtocol> map = HttpServer.getRpcClient();
            HMAMonitorProtocol p = map.get(CLUSTER);
            p.recoverAlarm(CLUSTER);
        }
    }
}
