package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.mr.BaseDriver;
import info.xiaohei.www.mr.JobInitModel;
import info.xiaohei.www.mr.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/24.
 */
public class Recommend {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        //计算用户评分矩阵
        String userScoreMatrixInpath = Util.HDFS + "/data/3-recommend/small1.csv";
        String userScoreMatrixOutpath = Util.HDFS + "/out/3-recommend/userScoreMatrix";
        JobInitModel userScoreMatrixJob = new JobInitModel(new String[]{userScoreMatrixInpath}, userScoreMatrixOutpath
                , conf, "CalcUserScoreMatrix", Recommend.class, UserScoreMatrixMapper.class, Text.class, Text.class
                , UserScoreMatrixReducer.class, Text.class, Text.class);

        //计算物品同现矩阵
        String itermOccurrenceOutpath = Util.HDFS + "/out/3-recommend/itermOccurrenceMatrix";
        JobInitModel itermOccurrenceMatrixJob = new JobInitModel(new String[]{userScoreMatrixOutpath}, itermOccurrenceOutpath
                , conf, "CalcItermOccurrenceMatrix", Recommend.class, ItermOccurrenceMapper.class, Text.class, IntWritable.class
                , ItermOccurrenceReducer.class, Text.class, IntWritable.class);

        //计算推荐结果
        String recommendOutpath = Util.HDFS + "/out/3-recommend/recommend";
        JobInitModel recommendJob = new JobInitModel(new String[]{itermOccurrenceOutpath, userScoreMatrixOutpath}
                , recommendOutpath, conf, "recommend", Recommend.class, RecommendMapper.class, Text.class, DoubleWritable.class
                , RecommendReducer.class, Text.class, Text.class);

        BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob, itermOccurrenceMatrixJob, recommendJob});
    }
}
