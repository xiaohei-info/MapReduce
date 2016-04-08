package info.xiaohei.www.mr.invertedindex;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by xiaohei on 16/3/20.
 * 实现简单的倒排索引
 */
public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void map(
                LongWritable key,
                Text value,
                org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            //FileSplit类从context上下文中得到，可以获得当前读取的文件的路径
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            //文件路径为hdfs://hadoop:9000/ii/a.txt
            //根据/分割取最后一块即可得到当前的文件名
            String[] fileNames = fileSplit.getPath().toString().split("/");
            String fileName = fileNames[fileNames.length - 1];
            for (String d : data) {
                k.set(d + "->" + fileName);
                v.set("1");
                context.write(k, v);
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            //分割文件名和单词
            String[] wordAndPath = key.toString().split("->");
            //统计出现次数
            int counts = 0;
            for (Text t : values) {
                counts += Integer.parseInt(t.toString());
            }
            //组成新的key-value输出
            k.set(wordAndPath[0]);
            v.set(wordAndPath[1] + "->" + counts);
            context.write(k, v);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String res = "";
            for (Text text : values) {
                res += text.toString() + "\r";
            }
            v.set(res);
            context.write(key, v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = HadoopUtil.HDFS + "/data/7-invertedindex/data.txt";
        String outPath = HadoopUtil.HDFS + "/out/7-invertedindex";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "InvertedIndex", InvertedIndex.class
                , null, InvertedIndexMapper.class, Text.class, Text.class, null, InvertedIndexCombiner.class
                , InvertedIndexReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        InvertedIndex.run();
    }
}
