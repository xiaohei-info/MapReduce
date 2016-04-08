package info.xiaohei.www.mr.posnet;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.JobInitModel;
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
        String[] inPath = new String[]{"hdfs://localhost:9000/data/1-kpi/*"};
        String outPath = "hdfs://localhost:9000/out/1-kpi/browser";
        String jobName = "posnet";

        if (args[2].equals("1")) {
            JobInitModel job = new JobInitModel(inPath, outPath, conf, null, jobName
                    , Posnet.class, null, Mapper.class, Text.class, Text.class, null, null, TotalReducer.class
                    , NullWritable.class, Text.class);
            BaseDriver.initJob(new JobInitModel[]{job});
        } else {
            JobInitModel job = new JobInitModel(inPath, outPath, conf, null, jobName
                    , Posnet.class, null, Mapper.class, Text.class, Text.class, null, null, Reducer.class
                    , NullWritable.class, Text.class);
            BaseDriver.initJob(new JobInitModel[]{job});
        }
    }
}
