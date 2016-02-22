package info.xiaohei.www.mr.kpi.ips;

import info.xiaohei.www.mr.kpi.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * 统计每个页面的独立访问ip数
 */
public class IpsCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Driver.InitJob("hdfs://localhost:9000/data/1-kpi/*", "hdfs://localhost:9000/out/1-kpi/ips"
                , new Configuration(), "ips", IpsCount.class, Mapper.class, Text.class, Text.class
                , info.xiaohei.www.mr.kpi.ips.Reducer.class
                , Text.class, IntWritable.class);
    }
}
