package info.xiaohei.www.mr.kpi.ips;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by xiaohei on 16/2/21.
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, IntWritable> {

    IntWritable count = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //使用set集合记性独立ip统计
        Set<String> ips = new HashSet<String>();
        for (Text ip : values) {
            ips.add(ip.toString());
        }
        count.set(ips.size());
        context.write(key, count);
    }
}
