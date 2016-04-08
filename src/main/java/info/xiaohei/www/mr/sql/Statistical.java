package info.xiaohei.www.mr.sql;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiaohei on 16/3/20.
 * 计算最大/最小/平均值
 * sql示例:
 * select avg(age) as avg,max(age) as max,min(age) as min from xxx;
 */
public class Statistical {
    public static class StatisticalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text("key");
        IntWritable v = new IntWritable();

        /**
         * 数据格式为name    age
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = HadoopUtil.SPARATOR.split(value.toString());
            v.set(Integer.parseInt(tokens[1]));
            context.write(k, v);
        }
    }

    public static class StatisticalReducer extends Reducer<Text, IntWritable, NullWritable, MapWritable> {
        MapWritable v = new MapWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            int min = Integer.MAX_VALUE;
            int sum = 0;
            int count = 0;
            for (IntWritable value : values) {
                if (max < value.get()) {
                    max = value.get();
                }
                if (min > value.get()) {
                    min = value.get();
                }
                sum += value.get();
                count++;
            }
            v.put(new Text("avg"), new IntWritable(sum / count));
            v.put(new Text("max"), new IntWritable(max));
            v.put(new Text("min"), new IntWritable(min));
            context.write(NullWritable.get(), v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = HadoopUtil.HDFS + "/data/6-sql/statistical/data.txt";
        String outPath = HadoopUtil.HDFS + "/out/6-sql/statistical";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "Statistical", Statistical.class
                , null, StatisticalMapper.class, Text.class, IntWritable.class, null, null
                , StatisticalReducer.class, NullWritable.class, MapWritable.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Statistical.run();
    }
}
