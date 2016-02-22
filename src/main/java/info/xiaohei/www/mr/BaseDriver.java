package info.xiaohei.www.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Created by xiaohei on 16/2/22.
 * 驱动程序设置类,在各个mr的驱动程序中调用此类的InitJob方法即可初始化Job
 */
public class BaseDriver {
    /**
     * 根据不同mr作业的不同参数初始化Job提供使用
     *
     * @param inPath           程序的属兔目录
     * @param outPath          程序的输出目录
     * @param conf             配置信息
     * @param jobName          该job的名称
     * @param jarClass         mr的驱动程序类
     * @param mapper           mapper的实现类
     * @param mapOutKeyClass   mapper输出的key类型
     * @param mapOutValueClass mapper输出的value类型
     * @param reducer          reducer的实现类
     * @param reduceOutKeyClass reduce输出的key类型
     * @param reduceOutValueClass reduce输出的value类型
     *                         <p/>
     *                         TODO:可根据需要进行修改,如添加自定义的分组,排序,规约类的设置
     */
    public static void InitJob(String inPath, String outPath
            , Configuration conf
            , String jobName
            , Class<?> jarClass
            , Class<? extends Mapper> mapper
            , Class<?> mapOutKeyClass
            , Class<?> mapOutValueClass
            , Class<? extends org.apache.hadoop.mapreduce.Reducer> reducer
            , Class<?> reduceOutKeyClass
            , Class<?> reduceOutValueClass
    ) throws IOException, ClassNotFoundException, InterruptedException {
        //检查hdfs上的输出目录
        FileSystem fs = FileSystem.get(conf);
        Path fileOutPath = new Path(outPath);
        if (fs.exists(fileOutPath)) {
            fs.delete(fileOutPath, true);
        }

        //初始化job
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(jarClass);
        //设置输入路径和format类
        FileInputFormat.setInputPaths(job, new Path(inPath));
        job.setInputFormatClass(TextInputFormat.class);

        //mapper类相关设置
        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(mapOutKeyClass);
        job.setMapOutputValueClass(mapOutValueClass);

        //reducer类相关设置
        job.setReducerClass(reducer);
        job.setOutputKeyClass(reduceOutKeyClass);
        job.setOutputValueClass(reduceOutValueClass);

        //设置输出目录和输出的format类
        FileOutputFormat.setOutputPath(job, fileOutPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        //提交作业
        job.waitForCompletion(true);
    }
}
