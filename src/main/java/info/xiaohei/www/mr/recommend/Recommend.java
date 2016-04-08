package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import info.xiaohei.www.mr.recommend.sort.SortData;
import info.xiaohei.www.mr.recommend.sort.SortMapper;
import info.xiaohei.www.mr.recommend.test.RecommendScoreMapper;
import info.xiaohei.www.mr.recommend.test.RecommendScoreReducer;
import info.xiaohei.www.mr.recommend.test.TransferUserScoreMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
        if (args.length < 1) {
            System.err.println("enter 0 or 1,0:use distributed cache,1:use normal map");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        //计算用户评分矩阵
        String userScoreMatrixInpath = HadoopUtil.HDFS + "/data/3-recommend/small1.csv";
        String userScoreMatrixOutpath = HadoopUtil.HDFS + "/out/3-recommend/userScoreMatrix";
        JobInitModel userScoreMatrixJob = new JobInitModel(new String[]{userScoreMatrixInpath}, userScoreMatrixOutpath
                , conf, null, "CalcUserScoreMatrix", Recommend.class, null, UserScoreMatrixMapper.class, Text.class, Text.class
                , null, null, UserScoreMatrixReducer.class, Text.class, Text.class);

        //计算物品同现矩阵
        String itermOccurrenceOutpath = HadoopUtil.HDFS + "/out/3-recommend/itermOccurrenceMatrix";
        JobInitModel itermOccurrenceMatrixJob = new JobInitModel(new String[]{userScoreMatrixOutpath}, itermOccurrenceOutpath
                , conf, null, "CalcItermOccurrenceMatrix", Recommend.class, null, ItermOccurrenceMapper.class, Text.class, IntWritable.class
                , null, null , ItermOccurrenceReducer.class, Text.class, IntWritable.class);

        if (args[0].equals("0")) {
            //计算推荐结果
            String recommendOutpath = HadoopUtil.HDFS + "/out/3-recommend/recommend";
            Job job = Job.getInstance(conf);
            job.addCacheFile(new URI(itermOccurrenceOutpath + "/part-r-00000#itermOccurrenceMatri"));
            JobInitModel recommendJob = new JobInitModel(new String[]{userScoreMatrixOutpath}
                    , recommendOutpath, conf, job, "recommend", Recommend.class, null, RecommendMapper.class, Text.class, DoubleWritable.class
                    , null, null, RecommendReducer.class, Text.class, Text.class);

            String sortOutpath = HadoopUtil.HDFS + "/out/3-recommend/sortedResult";
            JobInitModel sortJob = new JobInitModel(new String[]{recommendOutpath}
                    , sortOutpath, conf, null, "sortRecommend", Recommend.class, null, SortMapper.class, SortData.class, NullWritable.class
                    , null, null, null, null, null);

            BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob, itermOccurrenceMatrixJob, recommendJob, sortJob});
        } else {
            String transferUserScoreOutpath = HadoopUtil.HDFS + "/out/3-recommend/transferUserScore";
            JobInitModel transferUserScoreJob = new JobInitModel(new String[]{userScoreMatrixOutpath}, transferUserScoreOutpath
                    , conf, null, "TransferUserScore", Recommend.class, null, TransferUserScoreMapper.class, Text.class, Text.class
                    , null, null , null, null, null);

            //计算推荐结果
            String recommendOutpath = HadoopUtil.HDFS + "/out/3-recommend/recommend";
            JobInitModel recommendJob = new JobInitModel(new String[]{transferUserScoreOutpath, itermOccurrenceOutpath}
                    , recommendOutpath, conf, null, "recommend", Recommend.class, null, RecommendScoreMapper.class, Text.class
                    , Text.class, null, null, RecommendScoreReducer.class, Text.class, Text.class);
            BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob, itermOccurrenceMatrixJob, transferUserScoreJob, recommendJob});
        }


    }
}
