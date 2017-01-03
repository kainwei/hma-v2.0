package hma.monitor.collection.custom;

import hma.monitor.MonitorManager;

import hma.util.LOG;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by weizhonghui on 2016/11/27.
 */
public class mr2 {
    public void submit(){
        boolean jobSuccessful = false;
        hma.conf.Configuration hmaConf = MonitorManager.getGlobalConf();
        Date submitTime = new Date();
        Date completeTime = null;
        boolean jobError = false;
        SimpleDateFormat jobDateFormat =
                new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String jobTimestampID = jobDateFormat.format(System.currentTimeMillis());
        String hadoopConfDir = "conf/CLUSTER-01";
        //MonitorManager.getMonitorManager().getConf().get(
        //       "hadoop.client.conf.dir");
        Configuration conf = new Configuration();
        conf.addResource(new Path(hadoopConfDir, "core-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "mapred-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "yarn-site.xml"));
        conf.addResource(new Path(hadoopConfDir, "hadoop-default.xml"));
        conf.addResource(new Path(hadoopConfDir, "hadoop-site.xml"));
        //System.out.println("check conf " + conf.get("fs.defaultFS"));
        Job job = null;
        try {
            job = Job.getInstance(conf, "HmaTest");
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(Mapper.class);
            job.setReducerClass(Reducer.class);
            job.setNumReduceTasks(1);
            //job.setJar(hmaConf.get("probing.job.jar.path", "../../lib/hma.jar"));
            FileInputFormat.addInputPath(job, new Path("/monitor/test"));
            FileOutputFormat.setOutputPath(job, new Path("/monitor/out." + jobTimestampID));
            try {
                job.submit();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            String jobID = job.getJobID().toString();

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

//LOG.info("map " + Math.round(job.mapProgress() * 100) + "%\t" +
//"reduce " + Math.round(job.reduceProgress() * 100) + "%");
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
            System.out.println("job is complete " + job.isComplete() + " job is successful " + job.isSuccessful());



            if (!job.isSuccessful()) {
                jobSuccessful = false;
                LOG.warn("Job not Successful: " + jobID + " JobQueue: " );
            } else {
                jobSuccessful = true;
                LOG.info("Job Complete Successfully: " + jobID + " JobQueue: " );
            }

            jobError = false;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            if (jobError && (job != null)) {
                try {
                    //LOG.error("There's something wrong. Kill job: " + jobID + " JobQueue: " + queueName);
                    job.killJob();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }
    public static void main(String [] args ){
        mr2 m = new mr2();
        m.submit();

    }
}

