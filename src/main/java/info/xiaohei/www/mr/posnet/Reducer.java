package info.xiaohei.www.mr.posnet;

import info.xiaohei.www.HadoopUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * map阶段过来的数据格式为key:imsi,timeflag value:pos,unixtime
 * 1.按unixtime从小到大进行排序
 * 2.添加OFF位的unixtime(当前时段的最后时间)
 * 3.从大到小一次相减得到每个位置的停留时间
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {

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
            //循环输出
            for (Map.Entry<String, Float> entry : resMap.entrySet()) {
                String builder = imsi + "|" +
                        timeflag + "|" +
                        entry.getKey() + "|" +
                        entry.getValue();
                out.set(builder);
                context.write(NullWritable.get(), out);
            }
        } catch (ParseException e) {
            context.getCounter(Counter.TIMEFORMATERR).increment(1);
        }
    }
}
