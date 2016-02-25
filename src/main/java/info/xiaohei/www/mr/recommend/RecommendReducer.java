package info.xiaohei.www.mr.recommend;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/24.
 * 汇总统计各个用户对各个iterm的喜好度
 */
public class RecommendReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    Text userId = new Text();
    Text itermScore = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double totalScore = 0.0;
        for (DoubleWritable v : values) {
            totalScore += v.get();
        }
        String[] strArr = key.toString().split(":");
        userId.set(strArr[0]);
        itermScore.set(strArr[1] + ":" + totalScore);
        context.write(userId, itermScore);
    }
}
