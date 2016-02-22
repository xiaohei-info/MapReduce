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
        if (args.length != 2) {
            System.err.println("");
            System.err.println("Usage: Posnet <date> <timepoint>");
            System.err.println("Example: Posnet 2016-02-21 09-18-24");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("date", args[0]);
        conf.set("timepoint", args[1]);
        BaseDriver.InitJob("hdfs://localhost:9000/data/2-posnet", "hdfs://localhost:9000/out/2-posnet"
                , conf, "posnet", Posnet.class, Mapper.class, Text.class, Text.class, Reducer.class
                , NullWritable.class, Text.class);
    }
}
