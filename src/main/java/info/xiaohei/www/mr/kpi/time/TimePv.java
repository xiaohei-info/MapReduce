package info.xiaohei.www.mr.kpi.time;

import info.xiaohei.www.mr.kpi.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * 用户访问时间统计
 */
public class TimePv {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Driver.InitJob("hdfs://localhost:9000/data/1-kpi/*", "hdfs://localhost:9000/out/1-kpi/time"
                , new Configuration(), "time-pv", TimePv.class, Mapper.class, Text.class, IntWritable.class, Reducer.class
                , Text.class, IntWritable.class);
    }
}
