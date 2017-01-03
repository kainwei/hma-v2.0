/**
 * 
 */
package hma.monitor.collection.custom;

import hma.monitor.MonitorManager;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemWorker;
import hma.monitor.collection.metric.CollectionMetricPool;
import hma.monitor.collection.metric.CollectionMetricRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author guoyezhi
 *
    通过rpc方式，调用copyfromlocal上传一个文件再删除，监控其可写性
 *
 */
public class HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker extends MonitorItemWorker<
		Map<HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker.BenchmarkOP, Long>, 
		Map<HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker.BenchmarkOP, Long>> {
	
	public static final String className = 
		HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker.class.getSimpleName();
	
	public static final Log LOG = 
	    LogFactory.getLog(HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker.class);
	
	public static final String MONITOR_ITEM_NAME = 
		"HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark";
	
	private static SimpleDateFormat dateFormat = 
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static SimpleDateFormat fileIDDateFormat = 
		new SimpleDateFormat("yyyyMMddHHmmss");
	
	private Configuration hadoopConf = null;
	
	
	public HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker() {
		String hadoopConfDir = 
			MonitorManager.getMonitorManager().getConf().get(
					"hadoop.client.conf.dir");
		hadoopConf = new Configuration(false);
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-default.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-site.xml"));
	}
	
	
	public static enum BenchmarkOP {
		CopyFromLocal, CopyToLocal, Remove, RemoveRecursively
	}
	
	
	@Override
	public MonitorItemData<Map<BenchmarkOP, Long>> collect() {
		
		Map<BenchmarkOP, Long> testResult = new HashMap<BenchmarkOP, Long>();
		
		FileSystem fs = null;
		StringBuilder sb = new StringBuilder();
		long uploadBegTimestamp = 0;
		long uploadEndTimestamp = 0;
		long removeBegTimestamp = 0;
		long removeEndTimestamp = 0;
		
		try {
			sb.append("\n\t[" + dateFormat.format(new Date()) + " - " 
					+ className + "] Getting DistributedFileSystem Proxy ...");
			fs = FileSystem.get(hadoopConf);
			sb.append("\n\t[" + dateFormat.format(new Date()) + " - " 
					+ className + "] Have got DistributedFileSystem Proxy");
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
		Path src = new Path(
				System.getProperty("hma.home.dir") + "/benchmark/hdfs.copyfromlocal.benchmark");
		System.out.println("kain's log about FileUpload src" + src);
		uploadBegTimestamp = System.currentTimeMillis();
		Path dst = new Path(
				"/monitor/hma4hdfs/test." + fileIDDateFormat.format(new Date(uploadBegTimestamp)));
		System.out.println("kain's log about FileUpload dst" + dst);
		try {

			sb.append("\n\t[" + dateFormat.format(new Date(uploadBegTimestamp)) + " - "
					+ className + "] Uploading benchmark file to " + dst + " ...");
			fs.copyFromLocalFile(false, false, src, dst);
			uploadEndTimestamp = System.currentTimeMillis();
			sb.append("\n\t[" + dateFormat.format(new Date(uploadEndTimestamp)) + " - "
					+ className + "] Have uploaded benchmark file to " + dst);

			System.out.println("kain's log about FileUpload sb" + sb.toString());

			testResult.put(BenchmarkOP.CopyFromLocal, uploadEndTimestamp - uploadBegTimestamp);

			CollectionMetricPool.offerRecord(
					new CollectionMetricRecord(
							MonitorManager.getMonitoredClusterName(),
							"HDFS_RPC_FileSystem_UploadBenchmarkFile",
							uploadBegTimestamp,
							uploadEndTimestamp - uploadBegTimestamp));

			try {
				TimeUnit.MINUTES.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
//
//            removeBegTimestamp = System.currentTimeMillis();
//			sb.append("\n\t[" + dateFormat.format(new Date(removeBegTimestamp)) + " - "
//					+ className + "] Removing benchmark file " + dst + " ...");
//			boolean res = fs.delete(dst, false);
//			removeEndTimestamp = System.currentTimeMillis();
//			if (res) {
//				sb.append("\n\t[" + dateFormat.format(new Date(removeEndTimestamp)) + " - "
//						+ className + "] Have removed benchmark file " + dst);
//			}
//
//			System.out.println("kain's log about FileUpload remove sb" + sb.toString());
//			testResult.put(BenchmarkOP.Remove, removeEndTimestamp - removeBegTimestamp);
//			System.out.println("kain's log about FileUpload testResult" + testResult.toString());
//
//			CollectionMetricPool.offerRecord(
//					new CollectionMetricRecord(
//							MonitorManager.getMonitoredClusterName(),
//							"HDFS_RPC_FileSystem_RemoveBenchmarkFile",
//							removeBegTimestamp,
//							removeEndTimestamp - removeBegTimestamp));

		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		finally {
			/*
			 * Notice:
			 *     Should not close DistributedFileSystem which
			 *     is created by invoking static method
			 *     FileSystem.get(Configuration), otherwise the
			 *     other threads which are using DistributedFileSystem
			 *     will go wrong!
			 *
			 * if (fs != null) {
			 *     try {
			 *         fs.close();
			 *     } catch (IOException e) {
			 *         e.printStackTrace();
			 *     }
			 * }
			 */
		}

		LOG.info(sb.toString());
		
		return new MonitorItemData<Map<BenchmarkOP,Long>>(
				MONITOR_ITEM_NAME, testResult, uploadBegTimestamp);
		
	}
	
	@Override
	public MonitorItemData<Map<BenchmarkOP, Long>> extract(
			MonitorItemData<Map<BenchmarkOP, Long>> rawData) {
		return rawData;
	}
	
	
	/**
	 * for unit testing
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker worker = 
			new HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker();
		worker.extract(worker.collect());
	}
	
}

