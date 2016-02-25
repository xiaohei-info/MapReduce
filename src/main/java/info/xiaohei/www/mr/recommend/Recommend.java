package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.mr.BaseDriver;
import info.xiaohei.www.mr.JobInitModel;
import info.xiaohei.www.mr.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/24.
 */
public class Recommend {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String userScoreMatrixInpath = Util.HDFS + "/data/3-recommend/small.csv";
        String userScoreMatrixOutpath = Util.HDFS + "/out/3-recommend/userScoreMatrix";
        String jobName = "CalcUserScoreMatrix";
        Configuration conf = new Configuration();
        JobInitModel userScoreMatrixJob = new JobInitModel(new String[]{userScoreMatrixInpath}, userScoreMatrixOutpath
                , conf, jobName, Recommend.class, UserScoreMatrixMapper.class, Text.class, Text.class
                , UserScoreMatrixReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob});
    }
}
