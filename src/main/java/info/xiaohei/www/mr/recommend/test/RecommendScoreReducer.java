package info.xiaohei.www.mr.recommend.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiaohei on 16/2/25.
 */
public class RecommendScoreReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> result = new HashMap<String, Double>();
        for (Text value : values) {
            String[] str = value.toString().split(",");
            if (result.containsKey(str[0])) {
                result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
            } else {
                result.put(str[0], Double.parseDouble(str[1]));
            }
        }

        for (String itemID : result.keySet()) {
            double score = result.get(itemID);
            v.set(itemID + "," + score);
            context.write(key, v);
        }
    }
}
