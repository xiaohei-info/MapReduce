package info.xiaohei.www.mr.recommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/25.
 * 将同现矩阵的每一行体现为一行字符串,形如用户的评分矩阵
 */
public class TransferOccurrenceReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String rowOccurrence = "";
        for (Text value : values) {
            rowOccurrence += "," + value.toString();
        }
        v.set(rowOccurrence.replaceFirst(",", ""));
        context.write(key, v);
    }
}
