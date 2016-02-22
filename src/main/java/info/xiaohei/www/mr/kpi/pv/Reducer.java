package info.xiaohei.www.mr.kpi.pv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/20.
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable pagePv = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        pagePv.set(sum);
        context.write(key, pagePv);
    }
}
