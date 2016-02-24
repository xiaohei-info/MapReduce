package info.xiaohei.www.mr.posnet;

import info.xiaohei.www.mr.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 */
public class Posnet {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if (args.length < 3) {
            System.err.println("");
            System.err.println("Usage: Posnet <date> <timepoint> <isTotal>");
            System.err.println("Example: Posnet 2016-02-21 09-18-24 1");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("date", args[0]);
        conf.set("timepoint", args[1]);
        //使用统计最长停留时间的reducer
        if (args[2].equals("1")) {
            BaseDriver.InitJob("hdfs://localhost:9000/data/2-posnet", "hdfs://localhost:9000/out/2-posnet"
                    , conf, "posnet", Posnet.class, Mapper.class, Text.class, Text.class, TotalReducer.class
                    , NullWritable.class, Text.class);
        } else {
            BaseDriver.InitJob("hdfs://localhost:9000/data/2-posnet", "hdfs://localhost:9000/out/2-posnet"
                    , conf, "posnet", Posnet.class, Mapper.class, Text.class, Text.class, Reducer.class
                    , NullWritable.class, Text.class);
        }
    }
}
