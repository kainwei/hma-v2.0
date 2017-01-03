package hma.monitor.collection.custom;

import hma.monitor.MonitorManager;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemWorker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm_MonitorItemWorker extends MonitorItemWorker<
		Map<HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm_MonitorItemWorker.BenchmarkOP, Boolean>,
		Map<HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm_MonitorItemWorker.BenchmarkOP, Boolean>> {

	public static final String className =
			HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm_MonitorItemWorker.class.getSimpleName();

	public static final Log LOG =
	    LogFactory.getLog(HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm_MonitorItemWorker.class);

	public static final String MONITOR_ITEM_NAME =
		"HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm";

	private static SimpleDateFormat dateFormat =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static SimpleDateFormat fileIDDateFormat =
		new SimpleDateFormat("yyyyMMddHHmmss");

	private Configuration hadoopConf = null;


	public HDFS_RPC_FileSystem_MkdirAndPutAndCatAndRm_MonitorItemWorker() {
		String hadoopConfDir =
			MonitorManager.getMonitorManager().getConf().get(
					"hadoop.client.conf.dir");
		hadoopConf = new Configuration(false);
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-default.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-site.xml"));
	}


	public static enum BenchmarkOP {
		 mkdir, putFile, catFile, rmAllB
	}


	@Override
	public MonitorItemData<Map<BenchmarkOP, Boolean>> collect() {
		Map<BenchmarkOP,Boolean> resMap = new HashMap<BenchmarkOP, Boolean>();
		Map<BenchmarkOP, Long> testResult = new HashMap<BenchmarkOP, Long>();

		FileSystem fs = null;
		StringBuilder sb = new StringBuilder();
		long uploadBegTimestamp = 0;
		long uploadEndTimestamp = 0;
		long removeBegTimestamp = 0;
		long removeEndTimestamp = 0;
		boolean mkdirB = true;
		boolean putFileB = true;
		boolean catFileB =  true;
		boolean rmAllB = true;

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
		uploadBegTimestamp = System.currentTimeMillis();


		Path dstDir = new Path("/monitor/hma4hdfs/testDir");

		Path dst = new Path(
				"/monitor/hma4hdfs/testDir/putTestFile" );

		Set<String> helpSet = new HashSet<String>();


		try {
			// 0. init remove dir

			System.out.println("kain's log init rm dstDir");
			boolean res = fs.delete(dstDir, true);
			System.out.println("init rm res is " + res);

			//1 . mkdir
			fs.mkdirs(dstDir);
			for(FileStatus fileS:fs.listStatus(new Path("/monitor/hma4hdfs/"))){
				String dirName = fileS.getPath().toUri().getPath();
				System.out.println("kain's log print dir " + dirName);
				helpSet.add(dirName);
				if(dirName.equals("/monitor/hma4hdfs/testDir")){
					//System.out.println("kain's log in equal");
					break;
				}
			}
			if(!helpSet.contains("/monitor/hma4hdfs/testDir")){
				mkdirB = false;
				resMap.put(BenchmarkOP.mkdir, mkdirB);
				return new MonitorItemData<Map<BenchmarkOP, Boolean>>(
						MONITOR_ITEM_NAME, resMap, uploadBegTimestamp);
			}

			try {
				TimeUnit.SECONDS.sleep(8);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}


			// 2. put file
            helpSet = new HashSet<String>();
			sb.append("\n\t[" + dateFormat.format(new Date(uploadBegTimestamp)) + " - "
					+ className + "] Uploading benchmark file to " + dst + " ...");

			fs.copyFromLocalFile(false, false, src, dst);

			uploadEndTimestamp = System.currentTimeMillis();
			sb.append("\n\t[" + dateFormat.format(new Date(uploadEndTimestamp)) + " - "
					+ className + "] Have uploaded benchmark file to " + dst);

			for(FileStatus fileS:fs.listStatus(new Path("/monitor/hma4hdfs/testDir"))){
				String fileName = fileS.getPath().toUri().getPath();
				System.out.println("kain's log print file name " + fileName);

				helpSet.add(fileName);
				if(fileName.equals("/monitor/hma4hdfs/testDir/putTestFile")){
					//System.out.println("kain's log in file equal");
					break;
				}
			}

			if(!helpSet.contains("/monitor/hma4hdfs/testDir/putTestFile")){
				putFileB = false;
			}
			System.out.println("kain's log putFileB is " + putFileB);

			try {
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}


			// 3. cat file
			if(putFileB){
				System.out.println("kain's log 3rd cat step ");
				FSDataInputStream fdis = fs.open(dst);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try {
					IOUtils.copyBytes(fdis, baos, hadoopConf, false);
				} finally {
					fdis.close();
				}
				String catRes = baos.toString();
				System.out.println("kain's log cat file content : " + catRes
						+ " size is " + catRes.length());
				if(!catRes.contains("hma4hdfs")){
					catFileB = false;
				}

			}


			try {
				TimeUnit.SECONDS.sleep(8);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// 4. remove dir
			System.out.println("kain's log to 4th step rm ");
			removeBegTimestamp = System.currentTimeMillis();
			sb.append("\n\t[" + dateFormat.format(new Date(removeBegTimestamp)) + " - "
					+ className + "] Removing benchmark Dir " + dstDir + " ...");
			res = fs.delete(dstDir, true);
			removeEndTimestamp = System.currentTimeMillis();
			for(FileStatus fileS:fs.listStatus(new Path("/monitor/hma4hdfs/"))){
				String dirName = fileS.getPath().toUri().getPath();
				if(dirName.equals("/monitor/hma4hdfs/testDir")){
					rmAllB=false;
					break;
				}
			}



			if (rmAllB) {
				sb.append("\n\t[" + dateFormat.format(new Date(removeEndTimestamp)) + " - "
						+ className + "] Have removed benchmark file " + dst);
			}
			resMap.put(BenchmarkOP.mkdir, mkdirB);
			resMap.put(BenchmarkOP.putFile, putFileB);
			resMap.put(BenchmarkOP.catFile, catFileB);
			resMap.put(BenchmarkOP.rmAllB, rmAllB);
			System.out.println("kain's log m + p + c + r  " + mkdirB + " "
					+ putFileB + " " + catFileB + " " + rmAllB);


		} catch (IOException e) {
			e.printStackTrace();
			//return null;
		} finally {
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

		return new MonitorItemData<Map<BenchmarkOP, Boolean>>(
				MONITOR_ITEM_NAME, resMap, uploadBegTimestamp);

	}

	@Override
	public MonitorItemData<Map<BenchmarkOP, Boolean>> extract(MonitorItemData<Map<BenchmarkOP, Boolean>> rawData) {
		return rawData;
	}





	/**
	 * for unit testing
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		/*HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker worker =
			new HDFS_RPC_FileSystem_FileUploadAndRemoveBenchmark_MonitorItemWorker();
		worker.extract(worker.collect());*/

	}

}

