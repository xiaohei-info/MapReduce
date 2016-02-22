package info.xiaohei.www.mr.kpi.pv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * 网站pv数统计
 */
public class PvDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        info.xiaohei.www.mr.kpi.Driver.InitJob("hdfs://localhost:9000/data/1-kpi/*", "hdfs://localhost:9000/out/1-kpi/pv"
                , new Configuration(), "pv", PvDriver.class, Mapper.class, Text.class, IntWritable.class, Reducer.class
                , Text.class, IntWritable.class);
    }
}
