package info.xiaohei.www.mr.kpi.pv;

import info.xiaohei.www.mr.kpi.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/20.
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text request_page = new Text();
    Kpi kpi = new Kpi();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kpi = Kpi.parse(value.toString());
        if (kpi.getIs_validate()) {
            request_page.set(kpi.getRequest_page());
            context.write(request_page, one);
        }
    }
}
