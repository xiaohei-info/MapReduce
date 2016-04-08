package info.xiaohei.www.mr.weiboad;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright © 2016 xiaohei, All Rights Reserved.
 * Email : chubbyjiang@gmail.com
 * Host : xiaohei.info
 * Created : 16/4/8 16:15
 * <p/>
 * 根据公式计算权重
 */
public class CalcTotal {

    public static class CalcTotalMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        Map<String, Integer> countMap = new HashMap<String, Integer>();
        Map<String, Integer> dfMap = new HashMap<String, Integer>();

        /**
         * 正式的map过程之前先获得count和df的数据
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            URI[] uris = context.getCacheFiles();
            if (uris != null) {
                for (URI uri : uris) {
                    //df文件
                    if (uri.getPath().endsWith("part-r-00000")) {
                        Path path = new Path(uri);
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(path.getName()));
                        String s = bufferedReader.readLine();
                        if (s.startsWith("count")) {
                            String[] words = s.split("\t");
                            countMap.put(words[0], Integer.parseInt(words[1]));
                        }
                    } else if (uri.getPath().endsWith("part-r-00001")) {
                        Path path = new Path(uri);
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(path.getName()));
                        String s;
                        while ((s = bufferedReader.readLine()) != null) {
                            String[] words = s.split("\t");
                            dfMap.put(words[0], Integer.parseInt(words[1]));
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = HadoopUtil.SPARATOR.split(value.toString());
            if (tokens.length > 2) {
                int tf = Integer.parseInt(tokens[1]);
                String[] ss = tokens[0].split("_");
                double w = tf * Math.log(countMap.get("count") / dfMap.get(ss[0]));
                NumberFormat numberFormat = NumberFormat.getNumberInstance();
                numberFormat.setMaximumFractionDigits(5);
                k.set(ss[1]);
                v.set(ss[0] + ":" + w);
                context.write(k, v);
            }
        }
    }

    public static class CalcTotalReducer extends Reducer<Text, Text, Text, Text> {

        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder res = new StringBuilder();
            for (Text value : values) {
                res.append(value.toString()).append("\t");
            }
            v.set(res.toString());
            context.write(key, v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        //设置输入目录为tf的输出结果
        String inPath = HadoopUtil.HDFS + "/out/8-weoboad/tf-and-n/part-r-00000";
        String outPath = HadoopUtil.HDFS + "/out/8-weiboad/df";
        Job job = Job.getInstance(conf);
        //将hdfs上的文件加入分布式缓存
        job.addCacheFile(new URI("hdfs://localhost:9000/out/8-weiboad/tf-and-n/part-r-00001"));
        job.addCacheFile(new URI("hdfs://localhost:9000/out/8-weiboad/df/part-r-00000"));

        JobInitModel jobInitModel = new JobInitModel(new String[]{inPath}, outPath
                , conf, job, "CalcTotal", CalcTotal.class
                , null, CalcTotalMapper.class, Text.class, Text.class, null, null
                , CalcTotalReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{jobInitModel});
    }
}
