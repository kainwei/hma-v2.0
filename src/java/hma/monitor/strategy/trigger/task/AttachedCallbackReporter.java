/**
 * 
 */
package hma.monitor.strategy.trigger.task;

import hma.conf.Configuration;
import hma.monitor.MonitorManager;
import hma.web.HMAJspHelper;

import java.util.Date;
import java.util.HashMap;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;


/**
 * @author chiwen01
 *
 */
public class AttachedCallbackReporter extends MonitorStrategyAttachedTask {
    
    private String callbackUrls[] = null;

    /**
     * @param taskWorker
     * @param workerArgs
     * @param args
     */
    public AttachedCallbackReporter(String taskWorker, String[] workerArgs,
                                    MonitorStrategyAttachedTaskArguments args) {
        super(taskWorker, workerArgs, args);
        this.callbackUrls = 
            MonitorManager.getGlobalConf().getStrings("alarm.callback.url");
    }
    
    private void doSendCallbackMsg(String message) {
        try {
            for (int i = 0; i < callbackUrls.length; i++) {
                URL url = new URL(callbackUrls[i]);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setDoOutput(true);
                conn.setDoInput(true);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("User-Agent", "HMA");
                conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                conn.connect();
                OutputStreamWriter alarmMessage = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
                alarmMessage.write(message);
                alarmMessage.flush();
                alarmMessage.close();
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()));
                String response;
                while ((response = in.readLine()) != null)
                    System.out.println("callback server "+ callbackUrls[i]+ " return:" + response);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        
        MonitorStrategyAttachedTaskArguments taskArgs = 
            getAttachedTaskArguments();
        String monitorStrategyName = 
            taskArgs.getMonitorStrategyName();
        String alarmLevel = taskArgs.getAlarmLevel();
        String targetSystemName = taskArgs.getTargetSystemName();
        String additionalInfo = taskArgs.getAdditionalInfo();
        String key = null;
        
        Configuration conf = MonitorManager.getGlobalConf();
        String alarmThres = conf.get(
                "callback.alarm.level.threshold", 
                "NOTICE");
        
        if (MonitorStrategyAttachedTaskAdapter.compareAlarmLevel(
                alarmLevel, alarmThres) <= 0) {
            return;
        }
        
        HashMap rule_data = new HashMap();
        JSONObject ns_data = new JSONObject();
        JSONObject data = new JSONObject();
        JSONObject postData = new JSONObject();
        JSONObject alarmInfo = new JSONObject();
        JSONArray ns_dataJsonArray = new JSONArray();
        JSONArray dataJsonArray = new JSONArray();
        StringBuilder rulenameBuilder = new StringBuilder();

        try {
            JSONObject additionalInfoJsonObject = new JSONObject(additionalInfo);
            try {
                additionalInfo = (String) additionalInfoJsonObject.get("additionalInfo");
            } catch(JSONException e) {
                e.getCause();
            }
            try {
                key = (String) additionalInfoJsonObject.get("key");
            } catch(JSONException e) {
                key = "";
                e.getCause();
            }
        } catch (Exception e) {
            key = "";
            e.printStackTrace();
        }
        try {
            int status = 1;
            if (taskArgs.getFullInfo().startsWith("恢复正常")) {
                status = 1;
            } else {
                status = 0;
            }
            rule_data.put("status", status);
            rule_data.put("time", taskArgs.getTimestamp().getTime()/1000);
    
            rulenameBuilder.append(targetSystemName + ":");
            rulenameBuilder.append(alarmLevel + ":");
            rulenameBuilder.append(HMAJspHelper.getMonitorStrategyAlias(monitorStrategyName));
            ns_data.put("rule_name", rulenameBuilder.toString());
            ns_data.put("rule_data", rule_data);
            alarmInfo.put("message", taskArgs.getKeyInfo());
            alarmInfo.put("key", key);
    
//            data.put("name_space", targetSystemName);
            data.put("name_space", additionalInfo);
            data.put("alarm_info", alarmInfo);
            ns_dataJsonArray.put(0, ns_data);
            data.put("ns_data", ns_dataJsonArray);
    
            postData.put("type", 2);
            postData.put("token","");
            postData.put("name", "HMA");
            postData.put("time", new Date().getTime()/1000);
            dataJsonArray.put(0,data);
            postData.put("data", dataJsonArray);
        } catch (JSONException jsonException) {
            jsonException.getCause();
        }

        doSendCallbackMsg(postData.toString());
        
    }
}
