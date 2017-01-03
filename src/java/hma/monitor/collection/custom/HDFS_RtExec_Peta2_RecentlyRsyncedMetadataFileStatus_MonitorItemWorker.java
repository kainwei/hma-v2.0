package hma.monitor.collection.custom;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.json.JSONException;
import org.json.JSONObject;




import hma.monitor.MonitorManager;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemWorker;
import hma.util.EasyWebGet;

public class HDFS_RtExec_Peta2_RecentlyRsyncedMetadataFileStatus_MonitorItemWorker 
extends MonitorItemWorker<List<Map<String,String>>, List<Map<String,String>>> {
	
	public static final String MONITOR_ITEM_NAME =
			"HDFS_RtExec_Peta2_RecentlyRsyncedMetadataFileStatus";
	
	
	Map<String,String> nnFmsMetaDataMap = new HashMap <String,String>();
	public static class ProcKiller extends Thread{
		private Process proc;
   
        public  ProcKiller(Process proc) 
        { 
            this.proc = proc; 
        } 
    
        public void run() {

            try {
                     Thread.sleep(10000);
            } catch (InterruptedException e) {
                     e.printStackTrace();
            }
                   proc.destroy();
       }
    }   
	
	class RtCollector extends Thread{
		 private String commandArgs = null;
		
		 public  RtCollector(String commandArgs) 
        { 
            this.commandArgs = commandArgs; 
            
        } 
		 private synchronized void doCollect(){
			 StringBuilder sb1 = new StringBuilder();
			 for(int retry = 1; retry < 4; retry++ ){
				 String host = commandArgs.split(":")[0];
				 String metaName = commandArgs.split(":")[1];
				 Runtime rt = null;
	            rt = Runtime.getRuntime();
	            
	            InputStream in = null;
	            BufferedReader reader = null;
	            
	            String [] command = {"lftp", host,"-e","cd  /home/work/hadoop-peta2-rsync/meta && ls -t " + metaName +".[0-9]*[0-9]  && quit"};
	           

	             int count = 0;
	             try {
				
				 Process proc =  rt.exec(command);
	            //System.out.println("exitValue is : " + proc.exitValue());
	            //ExecutorService exec = Executors.newCachedThreadPool();
	            //exec.execute(ProcKiller(proc));
	            Thread thread = new ProcKiller(proc);
	            thread.start();
	           /*
	           try{
	             proc.waitFor();
	           } catch (InterruptedException e) {
	           e.printStackTrace();
	           }*/
	            in = proc.getInputStream();

	            reader = new BufferedReader(new InputStreamReader(in));

	            String str = null;
	            count = 0;
	                   while ((str = reader.readLine()) != null) {
	                           count++;
	                           sb1.append(str + "\n");
	                           if(count>0)
	                               break;

	                   }
	                   if(count ==0){
	                           System.out.println(" Peta2 collect rsync info & checkpoint log error !+++++error+++++error+++++ and retry times is : " + retry );
	                   }
	           
	               


	            } catch (IOException e) {
	                 // TODO Auto-generated catch block
	                 e.printStackTrace();
	            }finally {
	                  try {
	                       if (reader != null) reader.close();
	                       if (in != null) in.close();
	                   } catch (IOException e) {
	                        e.printStackTrace();
	                    }
	           }
	             if(count > 0 )break;
	             try {
					RtCollector.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             
	             
				 
			 }

             nnFmsMetaDataMap.put(commandArgs, sb1.toString());
             //System.out.println("kain's log about checkpoint stat " + host + ":  \n" + sb1.toString());
          
   }
		 
		 
		 public void run() {
			 
			 doCollect();
		 }
	}
	
	public static String getHostName(String ipString){
		
		
		InetAddress ipAddr = null;
		
	try {
			
			ipAddr = InetAddress.getByName(ipString);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ipAddr.getHostName().toString();
	}
	

	@Override
	public MonitorItemData<List<Map<String,String>>> collect() {
		long acquisitionTimestamp = System.currentTimeMillis();


		Map <String,String> nnFmsMap = new HashMap<String,String>();  
		String adapterAddr = MonitorManager.getGlobalConf().get("dfs.peta2.adapter","nj01-nanling-hdfs.dmop.baidu.com"); 		
        String zkUrl = "http://" + adapterAddr + ":8070/zkinfo";
        String zkRes = null;
        JSONObject additionalInfo = new JSONObject();
        StringBuilder hostlistBuilder = new StringBuilder();
        try {
            zkRes = EasyWebGet.get(zkUrl);
        } catch (ClientProtocolException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        JSONObject zkJson = null;
        try {
            zkJson = new JSONObject(zkRes);
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Iterator iter = zkJson.keys();
        StringBuilder fmsSb = new StringBuilder();
        while (iter.hasNext()) {
            String zkKey = (String) iter.next();
            //System.out.println(zkKey);
            if (zkKey.contains("ACTIVE") ) {
                String avIp = null;
                String sbIp = null;
                try {
                	 if (  (((JSONObject) zkJson.get(zkKey)).get("lastNotNull")).equals(null)   ){
                         continue;
                    }
                    avIp = ((String) ((JSONObject) zkJson.get(zkKey)).get("lastNotNull")).replaceAll(":\\d+","");
                    sbIp = ((String) ((JSONObject) zkJson.get(zkKey.replace("ACTIVE", "STANDBY"))).get("lastNotNull")).replaceAll(":\\d+","");
                    String avHost = getHostName(avIp).replace(".baidu.com", "");
                    String sbHost = getHostName(sbIp).replace(".baidu.com", "");
                    nnFmsMap.put(avHost, sbHost);
                    if (hostlistBuilder.length() > 0) hostlistBuilder.append(",");
                    hostlistBuilder.append(avHost+","+sbHost);
                    //System.out.println(avHost + " " + sbHost);
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
               
            }
        }
        
        //collect_meta_data cmd = new collect_meta_data();
        
		String metaNames [] = {"edits","fsimage"};
		List<Thread> tList = new ArrayList<Thread>();
		 Iterator<Map.Entry<String, String>> nnFmsiter = nnFmsMap.entrySet().iterator();
		 while(nnFmsiter.hasNext()){
			 Map.Entry<String, String> entry = nnFmsiter.next();
			 
				String[] hosts = {entry.getKey(),entry.getValue()};
				for (String host:hosts){
					for(String metaName:metaNames){
						//Thread rc = new RtCollector(host+":"+metaName);
						Thread rc = new RtCollector(host+ ":" + metaName);
						rc.start();	
						tList.add(rc);
					}
				}
				
				
		 }
		 for(int i=0;i<tList.size();i++){
			 try {
				tList.get(i).join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		 
		
/*		 Iterator<Map.Entry<String, String>> nnFmsiter1 = nnFmsMap.entrySet().iterator();
		 while(nnFmsiter1.hasNext()){
			 Map.Entry<String, String> entry = nnFmsiter1.next();
			 for (String metaName:metaNames){
			   String avMM [] = nnFmsMetaDataMap.get(entry.getKey() + ":" + metaName).split("\\s+");
			   String sbMM [] = nnFmsMetaDataMap.get(entry.getValue() + ":" + metaName).split("\\s+");
			   System.out.println(entry.getKey() 
					              + " "
					              + avMM[avMM.length -4 ] + " " + avMM[avMM.length -3 ] + " "
					              + avMM[avMM.length -2 ] + " "	+ avMM[avMM.length -1 ]	  
					              + "      " 
					              + entry.getValue()
					              + " "
					              + sbMM[sbMM.length -4 ] + " " + sbMM[sbMM.length -3 ] + " "
					              + sbMM[sbMM.length -2 ] + " " + sbMM[sbMM.length -1 ]		  
					            		  );
			 }
		 }  */
	           
		List<Map<String,String>> resMapList = new ArrayList<Map<String,String>>();
		resMapList.add(nnFmsMap);
		resMapList.add(nnFmsMetaDataMap);
        try {
            additionalInfo.put("additionalInfo", hostlistBuilder.toString());
        } catch (JSONException e) {
            e.getCause();
        }
		
		return new MonitorItemData<List<Map<String,String>>>(
				MONITOR_ITEM_NAME, resMapList, acquisitionTimestamp, additionalInfo.toString());
		// TODO Auto-generated method stub
		
	}


	@Override
	public MonitorItemData<List<Map<String, String>>> extract(
			MonitorItemData<List<Map<String, String>>> rawData) {
		// TODO Auto-generated method stub
		return rawData;
	}



	
}
