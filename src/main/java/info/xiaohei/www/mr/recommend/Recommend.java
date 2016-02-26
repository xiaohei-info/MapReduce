package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.mr.BaseDriver;
import info.xiaohei.www.mr.HadoopUtil;
import info.xiaohei.www.mr.JobInitModel;
import info.xiaohei.www.mr.recommend.test.RecommendScoreMapper;
import info.xiaohei.www.mr.recommend.test.RecommendScoreReducer;
import info.xiaohei.www.mr.recommend.test.TransferUserScoreMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by xiaohei on 16/2/24.
 */
public class Recommend {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        //计算用户评分矩阵
        String userScoreMatrixInpath = HadoopUtil.HDFS + "/data/3-recommend/small1.csv";
        String userScoreMatrixOutpath = HadoopUtil.HDFS + "/out/3-recommend/userScoreMatrix";
        JobInitModel userScoreMatrixJob = new JobInitModel(new String[]{userScoreMatrixInpath}, userScoreMatrixOutpath
                , conf, null, "CalcUserScoreMatrix", Recommend.class, UserScoreMatrixMapper.class, Text.class, Text.class
                , UserScoreMatrixReducer.class, Text.class, Text.class);

        String transferUserScoreOutpath = HadoopUtil.HDFS + "/out/3-recommend/transferUserScore";
        JobInitModel transferUserScoreJob = new JobInitModel(new String[]{userScoreMatrixOutpath}, transferUserScoreOutpath
                , conf, null, "TransferUserScore", Recommend.class, TransferUserScoreMapper.class, Text.class, Text.class
                , null, null, null);

        //计算物品同现矩阵
        String itermOccurrenceOutpath = HadoopUtil.HDFS + "/out/3-recommend/itermOccurrenceMatrix";
        JobInitModel itermOccurrenceMatrixJob = new JobInitModel(new String[]{userScoreMatrixOutpath}, itermOccurrenceOutpath
                , conf, null, "CalcItermOccurrenceMatrix", Recommend.class, ItermOccurrenceMapper.class, Text.class, IntWritable.class
                , ItermOccurrenceReducer.class, Text.class, IntWritable.class);

        String recommendScoreOutpath = "/out/3-recommend/recommendScore";
        JobInitModel recommendScoreJob = new JobInitModel(new String[]{transferUserScoreOutpath, itermOccurrenceOutpath}
                , recommendScoreOutpath, conf, null, "RecommendScore", Recommend.class, RecommendScoreMapper.class, Text.class, Text.class
                , RecommendScoreReducer.class, Text.class, Text.class);


        String transferOccurrenceOutpath = HadoopUtil.HDFS + "/out/3-recommend/transferOccurrence";
        JobInitModel transferOccurrenceJob = new JobInitModel(new String[]{itermOccurrenceOutpath}, transferOccurrenceOutpath
                , conf, null, "TransferOccurrence", Recommend.class, TransferOccurrenceMapper.class, Text.class, Text.class
                , TransferOccurrenceReducer.class, Text.class, Text.class);

        //计算推荐结果
        String recommendOutpath = HadoopUtil.HDFS + "/out/3-recommend/recommend";
        Job job = Job.getInstance(conf);
        job.addCacheFile(new URI(itermOccurrenceOutpath + "/part-r-00000#itermOccurrenceMatri"));
        JobInitModel recommendJob = new JobInitModel(new String[]{userScoreMatrixOutpath}
                , recommendOutpath, conf, job, "recommend", Recommend.class, RecommendMapper.class, Text.class, DoubleWritable.class
                , RecommendReducer.class, Text.class, Text.class);

        BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob, itermOccurrenceMatrixJob, recommendJob});
    }
}
