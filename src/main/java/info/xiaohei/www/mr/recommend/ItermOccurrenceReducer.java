package info.xiaohei.www.mr.recommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/24.
 * 统计每个itermId组合出现的次数
 */
public class ItermOccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable resCount = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        resCount.set(sum);
        context.write(key, resCount);
    }
}
