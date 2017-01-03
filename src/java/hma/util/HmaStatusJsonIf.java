package hma.util;
import hma.web.HMAJspHelper;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

public class HmaStatusJsonIf extends  HttpServlet {

     private static final long serialVersionUID = 1L;

     public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException{  
		
		res.setContentType("text/html");  
		 PrintWriter out = res.getWriter();
		 out.println("Got Post");
	 } 
     
     public void doGet(HttpServletRequest request, HttpServletResponse res) throws IOException{ 
    	 res.setContentType("text/html");
		 PrintWriter out = res.getWriter();
    	 HMAJspHelper helper = new HMAJspHelper(); 
         SimpleDateFormat dateFormat =
                 new SimpleDateFormat("yyyy-MM-dd" + " " + "HH:mm:ss");

         String clusterName = request.getParameter("clusterName");
         String strategyName = request.getParameter("strategyName"); 
         if (clusterName == null || clusterName.equals("")) throw new NullPointerException("No clustername!");
         if (strategyName == null || strategyName.equals("")) throw new NullPointerException("No strategyName!");
         
        // clusterName = clusterName.toUpperCase();
         
         String endTime = dateFormat.format(new Date());
         String begTime = dateFormat.format(new Date( (new Date()).getTime() - 3600 * 1000));
         //out.println("cluster is: " + clusterName + " time is " +  " to" + dateFormat.format(new Date())  ); 
         //out.println("cluster is: " + clusterName + " time is " +  begTime + " to " + endTime.toString()  ); 

         Map<String, Integer> checkResult = null;
         
         try {
			checkResult = helper.checkDetectionResults(clusterName, begTime, endTime);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

         int detectionResult = checkResult.get(strategyName);
         String detectionResultStr = null;
         
         if (detectionResult == HMAJspHelper.DetectionResult_NODATA) {
                         detectionResultStr =
                                 "NODATA";
                 } else if (detectionResult == HMAJspHelper.DetectionResult_NORMAL) {
                         detectionResultStr =
                                 "NORMAL";
                 } else {
                         detectionResultStr = "ANOMALY";
                 }

         //out.println("cluster is: " + clusterName + " strategyName is: " + strategyName + " result is: " + detectionResultStr );
         //out.print(  detectionResultStr );
         //JSONArray statusArr = new JSONArray();
         JSONObject clusterObj = new JSONObject();

         JSONObject strategyObj = new JSONObject();
         try {
			strategyObj.put(strategyName, detectionResultStr);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         //statusArr.put(strategyObj);
         //clusterObj.put(clusterName,statusArr ); 
        
			try {
				clusterObj.put(clusterName,strategyObj);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		
         //out.print(clusterObj.toString());
         //out.print("{\"" + clusterName + "\":{\"" + strategyName + "\":\"" + detectionResultStr + "\"}}");
         out.print(clusterObj.toString());
     }
	
}
