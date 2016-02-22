package info.xiaohei.www.mr.kpi;

import info.xiaohei.www.mr.BaseDriver;
import info.xiaohei.www.mr.kpi.browser.KpiBrowser;
import info.xiaohei.www.mr.kpi.browser.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * 驱动程序设置类,在各个mr的驱动程序中调用此类的InitJob方法即可初始化Job
 * <p/>
 * TODO:该类默认设置为运行两个job,第二个job为排序job,大部分情况下可能并不需要
 */
public class Driver {
    /**
     * 根据不同mr作业的不同参数初始化Job提供使用
     *
     * @param inPath           程序的属兔目录
     * @param outPath          程序的输出目录
     * @param jobName          该job的名称
     * @param conf             配置信息
     * @param jarClass         mr的驱动程序类
     * @param mapper           mapper的实现类
     * @param mapOutKeyClass   mapper输出的key类型
     * @param mapOutValueClass mapper输出的value类型
     * @param reducer          reducer的实现类
     * @param reduceOutKeyClass reduce输出的key类型
     * @param reduceOutValueClass reduce输出的value类型
     *                         TODO:可根据需要进行修改,如添加自定义的分组,排序,规约类的设置
     */
    public static void InitJob(String inPath, String outPath
            , Configuration conf
            , String jobName
            , Class<?> jarClass
            , Class<? extends Mapper> mapper
            , Class<?> mapOutKeyClass, Class<?> mapOutValueClass
            , Class<? extends org.apache.hadoop.mapreduce.Reducer> reducer
            , Class<?> reduceOutKeyClass
            , Class<?> reduceOutValueClass
    ) throws IOException, ClassNotFoundException, InterruptedException {
        BaseDriver.InitJob(inPath, outPath, conf, jobName, jarClass, mapper, mapOutKeyClass, mapOutValueClass, reducer
        ,reduceOutKeyClass,reduceOutValueClass);
        //第二个排序job,没有reduce过程
        Job newJob = Job.getInstance(conf, jobName + "-sort");
        newJob.setJarByClass(jarClass);

        FileInputFormat.setInputPaths(newJob, new Path(outPath + "/part-*"));
        newJob.setInputFormatClass(TextInputFormat.class);

        newJob.setMapperClass(SortMapper.class);
        newJob.setMapOutputKeyClass(SortKey.class);
        newJob.setMapOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(newJob, new Path(outPath + "/sort"));
        newJob.setOutputFormatClass(TextOutputFormat.class);

        newJob.waitForCompletion(true);
    }
}
