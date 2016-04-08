package info.xiaohei.www.mr.sql;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by xiaohei on 16/3/20.
 * 实现sql的join语句(多表操作)
 * Customer表结构为:
 * id   name
 * Orders表结构为:
 * id   cus_id
 * <p/>
 * sql示例:
 * select Customer.name,Orders.id from Customers left join Orders on Customers.id=Orders.cus_id
 */
public class Join {
    public static class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        IntWritable k = new IntWritable();
        Text v = new Text();

        /**
         * 根据map-reduce过程中分组和shuffle的特性,相同key的value最终会到一个reducer中处理
         * 所以只需要将join的关键字段作为key
         * value则是标识位+内容的格式
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String[] tokens = HadoopUtil.SPARATOR.split(value.toString());
            if (fileName.contains("Customers")) {
                k.set(Integer.parseInt(tokens[0]));
                //value中的key为0表示值是Customer表的
                v.set(new Text("0-" + tokens[1]));
            } else {
                k.set(Integer.parseInt(tokens[1]));
                //value中的key为1表示值是Orders表的
                v.set("1-" + new Text(tokens[0]));
            }
            context.write(k, v);
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable();

        /**
         * 处理的时候要注意是left join还是right join,本示例为left join
         */
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String customerName = "";
            Set<Integer> orders = new HashSet<Integer>();
            //提取customer和order表的信息
            //这里可以使用更为简便的方式,如map时使用多类型的value来区分,这里重点在于map和reduce中间连接的key部分
            for (Text value : values) {
                if (value.toString().startsWith("0")) {
                    customerName = value.toString().split("-")[1];
                }
                if (value.toString().startsWith("1")) {
                    orders.add(Integer.parseInt(value.toString().split("-")[1]));
                }
            }
            k.set(customerName);
            //left join 以左表Customer为基础
            for (Integer order : orders) {
                v.set(order);
                context.write(k, v);
            }
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = HadoopUtil.HDFS + "/data/6-sql/join/data.txt";
        String outPath = HadoopUtil.HDFS + "/out/6-sql/join";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "Join", Join.class
                , null, JoinMapper.class, IntWritable.class, Text.class, null, null
                , JoinReducer.class, Text.class, IntWritable.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Join.run();
    }
}
