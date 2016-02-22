package info.xiaohei.www.mr.kpi.browser;

import info.xiaohei.www.mr.kpi.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
    Text browser = new Text();
    IntWritable one = new IntWritable(1);
    Kpi kpi = new Kpi();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kpi = Kpi.parse(value.toString());
        if (kpi.getIs_validate()) {
            browser.set(kpi.getUser_agent());
            context.write(browser, one);
        }
    }
}
