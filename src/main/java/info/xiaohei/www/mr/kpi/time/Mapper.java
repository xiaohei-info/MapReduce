package info.xiaohei.www.mr.kpi.time;

import info.xiaohei.www.mr.kpi.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    Kpi kpi = new Kpi();
    Text time = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kpi = Kpi.parse(value.toString());
        if (kpi.getIs_validate()) {
            time.set(kpi.getRequest_time());
            context.write(time, one);
        }
    }
}
