package info.xiaohei.www.mr.kpi.ips;

import info.xiaohei.www.mr.BaseDriver;
import info.xiaohei.www.mr.JobInitModel;
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
        String[] inPath = new String[]{"hdfs://localhost:9000/data/1-kpi/*"};
        String outPath = "hdfs://localhost:9000/out/1-kpi/ips";
        Configuration conf = new Configuration();
        String jobName = "ips";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, jobName
                , IpsCount.class, Mapper.class, Text.class, Text.class, Reducer.class
                , Text.class, IntWritable.class);

        JobInitModel sortJob = new JobInitModel(new String[]{outPath + "/part-*"}, outPath + "/sort", conf
                , jobName + "sort", IpsCount.class, Mapper.class, Text.class, IntWritable.class, null, null, null);

        BaseDriver.initJob(new JobInitModel[]{job, sortJob});
    }
}
