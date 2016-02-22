package info.xiaohei.www.mr.kpi.browser;

import info.xiaohei.www.mr.kpi.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * 统计用户使用的客户端程序
 */
public class KpiBrowser {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Driver.InitJob("hdfs://localhost:9000/data/1-kpi/*", "hdfs://localhost:9000/out/1-kpi/browser"
                , new Configuration(), "browser-pv", KpiBrowser.class, Mapper.class, Text.class, IntWritable.class, Reducer.class
                , Text.class, IntWritable.class);
    }
}
