package info.xiaohei.www.mr.weiboad;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Copyright © 2016 xiaohei, All Rights Reserved.
 * Email : chubbyjiang@gmail.com
 * Host : xiaohei.info
 * Created : 16/4/8 16:05
 * <p/>
 * 在TF的基础上计算DF
 */
public class CalcDF {

    public static class CalcDFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("_");
            k.set(tokens[0]);
            context.write(k, v);
        }
    }

    public static class CalcDFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            v.set(sum);
            context.write(key, v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        //设置输入目录为tf的输出结果
        String inPath = HadoopUtil.HDFS + "/out/8-weoboad/tf-and-n/part-r-00000";
        String outPath = HadoopUtil.HDFS + "/out/8-weiboad/df";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "CalcDF", CalcDF.class
                , null, CalcDFMapper.class, Text.class, IntWritable.class, null, null
                , CalcDFReducer.class, Text.class, IntWritable.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
