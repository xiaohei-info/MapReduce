package info.xiaohei.www;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/22.
 * 驱动程序设置类,在各个mr的驱动程序中调用此类的InitJob方法即可初始化Job
 */
public class BaseDriver {
    /**
     * 根据不同mr作业的不同参数初始化Job提供使用
     *
     * @param jobs 一个或多个保存着job信息的JobInitModel对象
     *             TODO:可根据需要进行修改,如添加自定义的分组,排序,规约类的设置
     */
    public static void initJob(JobInitModel[] jobs
    ) throws IOException, ClassNotFoundException, InterruptedException {
        for (JobInitModel jobInitModel : jobs) {
            //检查hdfs上的输出目录
            FileSystem fs = FileSystem.get(jobInitModel.getConf());
            Path fileOutPath = new Path(jobInitModel.getOutPath());
            if (fs.exists(fileOutPath)) {
                fs.delete(fileOutPath, true);
            }

            //初始化job
            Job job;
            if (jobInitModel.getJob() == null) {
                job = Job.getInstance(jobInitModel.getConf(), jobInitModel.getJobName());
            } else {
                job = jobInitModel.getJob();
            }
            job.setJarByClass(jobInitModel.getJarClass());
            //设置输入路径和format类
            String[] inPathsStr = jobInitModel.getInPaths();
            Path[] inPaths = new Path[inPathsStr.length];
            for (int i = 0; i < inPathsStr.length; i++) {
                inPaths[i] = new Path(inPathsStr[i]);
            }
            FileInputFormat.setInputPaths(job, inPaths);
            if (jobInitModel.getInputFormatClass() != null) {
                job.setInputFormatClass(jobInitModel.getInputFormatClass());
            } else {
                job.setInputFormatClass(TextInputFormat.class);
            }

            //mapper类相关设置
            job.setMapperClass(jobInitModel.getMapper());
            job.setMapOutputKeyClass(jobInitModel.getMapOutKeyClass());
            job.setMapOutputValueClass(jobInitModel.getMapOutValueClass());

            //Partitioner设置
            if (jobInitModel.getPartitionerClass() != null) {
                job.setPartitionerClass(jobInitModel.getPartitionerClass());
            }

            //combiner设置
            if (jobInitModel.getCombinerClass() != null) {
                job.setCombinerClass(jobInitModel.getCombinerClass());
            }

            if (jobInitModel.getReducer() != null) {
                //reducer类相关设置
                job.setReducerClass(jobInitModel.getReducer());
                job.setOutputKeyClass(jobInitModel.getReduceOutKeyClass());
                job.setOutputValueClass(jobInitModel.getReduceOutValueClass());
            }

            //设置输出目录和输出的format类
            FileOutputFormat.setOutputPath(job, fileOutPath);
            job.setOutputFormatClass(TextOutputFormat.class);

            //提交作业
            job.waitForCompletion(true);
        }
    }
}
