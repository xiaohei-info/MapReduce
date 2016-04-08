package info.xiaohei.www.mr.sql;

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
 * Created by xiaohei on 16/3/20.
 * 实现group by语句
 * sql示例:select customer,sum(order_price) from orders group by customer
 */
public class GroupBy {
    public static class GroupByMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable();

        /**
         * 输入的数据格式为:
         * customer    order_price
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = HadoopUtil.SPARATOR.split(value.toString());
            k.set(tokens[0]);
            v.set(Integer.parseInt(tokens[1]));
            context.write(k, v);
        }
    }

    /**
     * 这段代码和普通的map-reduce代码是一样的
     * map-reduce过程中的分组过程会自动按照key进行分组,天然自带group by,所以将group by的关键字段作为key即可
     */
    public static class GroupByReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        String inPath = HadoopUtil.HDFS + "/data/6-sql/groupby/data.txt";
        String outPath = HadoopUtil.HDFS + "/out/6-sql/groupby";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "GroupBy", GroupBy.class
                , null, GroupByMapper.class, Text.class, IntWritable.class, null, null
                , GroupByReducer.class, Text.class, IntWritable.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        GroupBy.run();
    }
}
