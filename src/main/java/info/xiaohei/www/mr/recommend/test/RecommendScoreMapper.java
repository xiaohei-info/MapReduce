package info.xiaohei.www.mr.recommend.test;

import info.xiaohei.www.HadoopUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiaohei on 16/2/25.
 */
public class RecommendScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    private final static Map<String, List<ItermOccurrence>> itermOccurrenceMatrix = new HashMap<String, List<ItermOccurrence>>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = HadoopUtil.SPARATOR.split(value.toString());

        String[] v1 = tokens[0].split(":");
        String[] v2 = tokens[1].split(":");

        if (v1.length > 1) {// cooccurrence
            String itemID1 = v1[0];
            String itemID2 = v1[1];
            int num = Integer.parseInt(tokens[1]);

            List<ItermOccurrence> list;
            if (!itermOccurrenceMatrix.containsKey(itemID1)) {
                list = new ArrayList<ItermOccurrence>();
            } else {
                list = itermOccurrenceMatrix.get(itemID1);
            }
            list.add(new ItermOccurrence(itemID1, itemID2, num));
            itermOccurrenceMatrix.put(itemID1, list);
        }

        if (v2.length > 1) {
            // userVector
            String itemID = tokens[0];
            String userID = v2[0];
            double pref = Double.parseDouble(v2[1]);
            k.set(userID);
            for (ItermOccurrence co : itermOccurrenceMatrix.get(itemID)) {
                v.set(co.getItemId1() + "," + pref * co.getOccurrence());
                context.write(k, v);
            }
        }
    }
}
