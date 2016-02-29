package info.xiaohei.www.mr.paoding.special;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/29.
 * 对各个类别的分词进行汇总
 */
public class PaodingReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder totalStr = new StringBuilder();
        for (Text value : values) {
            totalStr.append(value.toString());
        }
        v.set(totalStr.toString());
        context.write(key, v);
    }
}
