package info.xiaohei.www.mr.kpi.browser;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.JobInitModel;
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
        String[] inPath = new String[]{"hdfs://localhost:9000/data/1-kpi/*"};
        String outPath = "hdfs://localhost:9000/out/1-kpi/browser";
        Configuration conf = new Configuration();
        String jobName = "browser-pv";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, jobName
                , KpiBrowser.class, null, Mapper.class, Text.class, IntWritable.class, null, null, Reducer.class
                , Text.class, IntWritable.class);

        JobInitModel sortJob = new JobInitModel(new String[]{outPath + "/part-*"}, outPath + "/sort", conf, null
                , jobName + "sort", KpiBrowser.class, null, Mapper.class, Text.class, IntWritable.class, null, null, null, null, null);

        BaseDriver.initJob(new JobInitModel[]{job, sortJob});
    }
}
