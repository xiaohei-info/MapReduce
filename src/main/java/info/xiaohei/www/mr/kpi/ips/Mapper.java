package info.xiaohei.www.mr.kpi.ips;

import info.xiaohei.www.mr.kpi.Kpi;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

    Kpi kpi = new Kpi();
    Text requestPage = new Text();
    Text remoteAddr = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kpi = Kpi.parse(value.toString());
        if (kpi.getIs_validate()) {
            requestPage.set(kpi.getRequest_page());
            remoteAddr.set(kpi.getRemote_addr());
            context.write(requestPage, remoteAddr);
        }
    }
}
