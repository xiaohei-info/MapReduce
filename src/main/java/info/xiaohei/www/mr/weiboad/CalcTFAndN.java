package info.xiaohei.www.mr.weiboad;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import net.paoding.analysis.analyzer.PaodingAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;

/**
 * Copyright © 2016 xiaohei, All Rights Reserved.
 * Email : chubbyjiang@gmail.com
 * Host : xiaohei.info
 * Created : 16/4/8 15:36
 * <p/>
 * 从原始微博数据中计算TF和N的值
 */
public class CalcTFAndN {
    public static class CalcTFAndNMapper extends Mapper<Text, LongWritable, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        //庖丁分词类
        PaodingAnalyzer analyzer = new PaodingAnalyzer();

        @Override
        protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            String[] tokens = HadoopUtil.SPARATOR.split(value.toString());
            //对文件内容进行分词
            //计算TF
            TokenStream tokenStream = analyzer.tokenStream("", new StringReader(tokens[1]));
            while (tokenStream.incrementToken()) {
                CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
                k.set(attribute.toString() + "_" + tokens[0]);
                context.write(k, v);
            }
            //计算N
            k.set("count");
            context.write(k, v);
        }
    }

    public static class CalcTFAndNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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

    /**
     * 让N的计算结果和TF的结果交给不同的reducer,这样才能输出在不同的文件中
     */
    public static class CountPartitioner extends HashPartitioner<Text, IntWritable> {

        /**
         * 采用4个reducer,count交给最后一个
         */
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            if (key.equals(new Text("count"))) {
                return 1;
            } else {
                //其余使用默认的分区方式,此时传递的分区数应该-1
                return super.getPartition(key, value, numReduceTasks - 1);
            }
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = HadoopUtil.HDFS + "/data/8-weoboad/data.txt";
        String outPath = HadoopUtil.HDFS + "/out/8-weiboad/tf-and-n";
        Job job = Job.getInstance();
        //设置reduce任务数
        job.setNumReduceTasks(2);
        JobInitModel jobInitModel = new JobInitModel(new String[]{inPath}, outPath, conf, job, "CalcTFAndN", CalcTFAndN.class
                , null, CalcTFAndNMapper.class, Text.class, IntWritable.class, CountPartitioner.class, null
                , CalcTFAndNReducer.class, Text.class, IntWritable.class);
        BaseDriver.initJob(new JobInitModel[]{jobInitModel});
    }
}
