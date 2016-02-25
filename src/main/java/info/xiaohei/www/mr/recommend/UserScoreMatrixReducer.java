package info.xiaohei.www.mr.recommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/24.
 * 将map的输出聚合为用户评分矩阵输出
 */
public class UserScoreMatrixReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //map输出的数据key为1,value为101:5.0
        String itermPers = "";
        for (Text itermPer : values) {
            itermPers += "," + itermPer.toString();
        }
        v.set(itermPers.replaceFirst(",", ""));
        context.write(key, v);
    }
}
