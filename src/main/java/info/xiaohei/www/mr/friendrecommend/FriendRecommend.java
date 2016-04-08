package info.xiaohei.www.mr.friendrecommend;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Copyright © 2016 xiaohei, All Rights Reserved.
 * Email : chubbyjiang@gmail.com
 * Host : xiaohei.info
 * Created : 16/4/6 14:51
 *
 * 查找社交关系中的二度关系
 */
public class FriendRecommend {
    public static class FriendRecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        /**
         * 将key和value都组合输出
         * */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = HadoopUtil.SPARATOR.split(value.toString());
            k.set(tokens[0]);
            v.set(tokens[1]);
            context.write(k, v);
            k.set(tokens[1]);
            v.set(tokens[0]);
            context.write(k, v);
        }
    }

    /**
     * 进行全排序
     * */
    public static class FriendRecommendReducer extends Reducer<Text, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> friends = new HashSet<String>();
            for (Text value : values) {
                friends.add(value.toString());
            }
            if (friends.size() > 1) {
                for (String f1 : friends) {
                    for (String f2 : friends) {
                        if (!f1.equals(f2)) {
                            k.set(f1);
                            v.set(f2);
                            context.write(k, v);
                        }
                    }
                }
            }
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = HadoopUtil.HDFS + "/data/7-friendrecommend/data.txt";
        String outPath = HadoopUtil.HDFS + "/out/7-friendrecommend";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "FriendRecommend", FriendRecommend.class
                , null, FriendRecommendMapper.class, Text.class, Text.class, null, null
                , FriendRecommendReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        FriendRecommend.run();
    }
}
