package hma.monitor.collection.custom;

import hma.monitor.MonitorManager;
import hma.monitor.collection.MonitorItemData;
import hma.monitor.collection.MonitorItemWorker;
import hma.monitor.collection.metric.CollectionMetricPool;
import hma.monitor.collection.metric.CollectionMetricRecord;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;




import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.hadoop.mapreduce.JobPriority.VERY_HIGH;


/**
 * @author kain
 * 
 */
public class MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker extends MonitorItemWorker<
		MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker.HMAProbingJobResult,
		MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker.HMAProbingJobResult> {
	
	public static final String className = 
		MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker.class.getSimpleName();
	
	public static final Log LOG =
	    LogFactory.getLog(MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker.class);
	
	public static final String MONITOR_ITEM_NAME = 
		"MapRed_RPC_ComputingStatus_AvailabilityProbingJob";
	
	private Configuration hadoopConf = null;
	
	private static SimpleDateFormat jobDateFormat = 
		new SimpleDateFormat("yyyyMMddHHmmssSSS");
	
	
	public MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker() {
		String hadoopConfDir =
				MonitorManager.getMonitorManager().getConf().get(
						"hadoop.client.conf.dir");

		hadoopConf = new Configuration(false);
		hadoopConf.addResource(new Path(hadoopConfDir, "core-site.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "mapred-site.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "yarn-site.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-default.xml"));
		hadoopConf.addResource(new Path(hadoopConfDir, "hadoop-site.xml"));
	}
	
	

	


	
	public static class HMAProbingJobResult {
		
		private JobID jobID = null;
		
		private String jobName = null;
		
		private String queueName = null;
		
		private boolean successful = false;
		
		private Date jobSubmitTime = null;
		
		private Date jobCompleteTime = null;
		
		public HMAProbingJobResult(
				JobID jobID,
				String jobName,
				String queueName,
				boolean successful,
				Date jobSubmitTime, 
				Date jobCompleteTime) {
			this.jobID = jobID;
			this.jobName = jobName;
			this.queueName = queueName;
			this.successful = successful;
			this.jobSubmitTime = jobSubmitTime;
			this.jobCompleteTime = jobCompleteTime;
		}
		
		public JobID getJobID() {
			return jobID;
		}
		
		public String getJobName() {
			return jobName;
		}
		
		public String getQueueName() {
			return queueName;
		}
		
		public boolean isSuccessful() {
			return successful;
		}

		public Date getJobSubmitTime() {
			return jobSubmitTime;
		}
		
		public Date getJobCompleteTime() {
			return jobCompleteTime;
		}
		
	}
	
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see hma.monitor.collection.MonitorItemWorker#collect()
	 */
	@Override
	public MonitorItemData<HMAProbingJobResult> collect() {

		boolean jobSuccessful = false;
		hma.conf.Configuration hmaConf = MonitorManager.getGlobalConf();
		Date submitTime = new Date();
		Date completeTime = null;
		boolean jobError = false;
		SimpleDateFormat jobDateFormat =
				new SimpleDateFormat("yyyyMMddHHmmssSSS");
		String jobTimestampID = jobDateFormat.format(System.currentTimeMillis());
		String hadoopConfDir =
		MonitorManager.getMonitorManager().getConf().get(
		       "hadoop.client.conf.dir");
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopConfDir, "core-site.xml"));
		conf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
		conf.addResource(new Path(hadoopConfDir, "mapred-site.xml"));
		conf.addResource(new Path(hadoopConfDir, "yarn-site.xml"));
		conf.addResource(new Path(hadoopConfDir, "hadoop-default.xml"));
		conf.addResource(new Path(hadoopConfDir, "hadoop-site.xml"));

		Job job = null;
		JobID jobID = null;
		String jobName = null;
		String queueName = "";

		try {
			job = Job.getInstance(conf, "HMA_ProbingJob_" + jobTimestampID);
			job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
			job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(org.apache.hadoop.mapreduce.Mapper.class);
			job.setReducerClass(org.apache.hadoop.mapreduce.Reducer.class);
			job.setNumReduceTasks(1);
            job.setPriority(VERY_HIGH);
			//job.setJar(hmaConf.get("probing.job.jar.path", "../../lib/hma.jar"));
			org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path("/monitor/test"));
			org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path("/monitor/out." + jobTimestampID));
			try {
				job.submit();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			jobID = job.getJobID();
			jobName = job.getJobName();

			System.out.println("job url: " + job.getTrackingURL() + " sch info: "
					+ job.getSchedulingInfo() + " job history: "
					+ job.getHistoryUrl() + " job name: " + job.getJobName()
					+ " job id: " + jobID);
			int tryTimes = 0;
			int maxTryTimes = conf.getInt("stream.report.maxretry", 300);

			while (true) {
				try {

					if (job.isComplete()) {
						completeTime = new Date();
						break;
					}
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
                        // nothing needed to do
					}

                    LOG.info("map " + Math.round(job.mapProgress() * 100) + "%\t" +
                        "reduce " + Math.round(job.reduceProgress() * 100) + "%");
					tryTimes = 0;


				} catch (IOException ioe) {
					if (tryTimes++ >= maxTryTimes) {
						// not an error
						jobError = false;
						break;
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				}
			}
			System.out.println("job is complete " + job.isComplete() +
					" job is successful " + job.isSuccessful());




			if (!job.isSuccessful()) {
				jobSuccessful = false;
				hma.util.LOG.warn("Job not Successful: " + jobID + " JobQueue: " );
			} else {
				jobSuccessful = true;
				hma.util.LOG.info("Job Complete Successfully: " + jobID + " JobQueue: " );
			}

			jobError = false;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}finally{
			if (jobError && (job != null)) {
				try {
					LOG.error("There's something wrong. Kill job: " + jobID + " JobQueue " );
					job.killJob();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}


		
		CollectionMetricPool.offerRecord(
				new CollectionMetricRecord(
						MonitorManager.getMonitoredClusterName(),
						MONITOR_ITEM_NAME, 
						submitTime.getTime(), 
						completeTime.getTime() - submitTime.getTime()));
		
		HMAProbingJobResult jobResult = 
			new HMAProbingJobResult(
					jobID, jobName, queueName, 
					jobSuccessful, submitTime, completeTime);
		
		return new MonitorItemData<HMAProbingJobResult>(
				MONITOR_ITEM_NAME, jobResult, submitTime.getTime());
		
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * hma.monitor.collection.MonitorItemWorker#extract(hma.monitor.collection
	 * .MonitorItemData)
	 */
	@Override
	public MonitorItemData<HMAProbingJobResult> extract(
			MonitorItemData<HMAProbingJobResult> rawData) {
		return rawData;
	}
	
	
	/**
	 * for unit testing
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker worker =
			new MapRed_RPC_ComputingStatus_AvailabilityProbingJob_MonitorItemWorker();
		HMAProbingJobResult jr = 
			worker.extract(worker.collect()).getData();
		System.out.println(jr.getJobID() + " " + jr.getJobName() + " " +
				jr.getQueueName() + " Successful:" + jr.isSuccessful() + " " +
				jr.getJobSubmitTime() + " " + jr.getJobCompleteTime());
	}
	
}

