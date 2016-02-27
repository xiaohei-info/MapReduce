package info.xiaohei.www.mr.posnet;

import info.xiaohei.www.HadoopUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by xiaohei on 16/2/23.
 * 只保留每个用户每个时间段停留时间最长的基站位置
 */
public class TotalReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {
    //要计算的时间
    String day;
    Text out = new Text();

    /**
     * reduce阶段只执行一次,用于参数初始化等
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.day = context.getConfiguration().get("date");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //使用TreeMap存储,key为unixtime,自动排序
        TreeMap<Long, String> sortedData = HadoopUtil.getSortedData(context, values);
        String[] ks = key.toString().split(",");
        String imsi = ks[0];
        String timeflag = ks[1];
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            //设置该数据所在的最后时段的unixtime
            Date offTimeflag = simpleDateFormat.parse(this.day + " " + timeflag.split("-")[1] + ":00:00");
            sortedData.put(offTimeflag.getTime() / 1000L, "OFF");
            //计算两两之间的时间间隔
            HashMap<String, Float> resMap = HadoopUtil.calcStayTime(sortedData);
            //将每个key(pos)所对应的values筛选出停留时间最长的
            String longestPos = "";
            Float longestTime = 0f;
            for (Map.Entry<String, Float> entry : resMap.entrySet()) {
                if (entry.getValue() > longestTime) {
                    longestPos = entry.getKey();
                    longestTime = entry.getValue();
                }
            }
            out.set(imsi + "|" + timeflag + "|" + longestPos + "|" + longestTime);
            context.write(NullWritable.get(), out);
        } catch (ParseException e) {
            context.getCounter(Counter.TIMEFORMATERR).increment(1);
        }
    }
}
