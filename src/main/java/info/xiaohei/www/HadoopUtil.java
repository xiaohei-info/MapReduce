package info.xiaohei.www;

import info.xiaohei.www.mr.posnet.Counter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Created by xiaohei on 16/2/23.
 * 通用工具类
 */
public class HadoopUtil {

    /**
     * 分隔符类型,使用正则表达式,表示分隔符为\t或者,
     * 使用方法为SPARATOR.split(字符串)
     */
    public static final Pattern SPARATOR = Pattern.compile("[\t,]");

    /**
     * HDFS路径的根目录
     */
    public static final String HDFS = "hdfs://localhost:9000";

    /**
     * 计算unixtime两两之间的时间差
     *
     * @param sortDatas key为unixtime,value为pos
     * @return key为pos, value为该pos的停留时间
     */
    public static HashMap<String, Float> calcStayTime(TreeMap<Long, String> sortDatas) {
        HashMap<String, Float> resMap = new HashMap<String, Float>();
        Iterator<Long> iter = sortDatas.keySet().iterator();
        Long currentTimeflag = iter.next();
        //遍历treemap
        while (iter.hasNext()) {
            Long nextTimeflag = iter.next();
            float diff = (nextTimeflag - currentTimeflag) / 60.0f;
            //超过60分钟过滤不计
            if (diff <= 60.0) {
                String currentPos = sortDatas.get(currentTimeflag);
                if (resMap.containsKey(currentPos)) {
                    resMap.put(currentPos, resMap.get(currentPos) + diff);
                } else {
                    resMap.put(currentPos, diff);
                }
            }
            currentTimeflag = nextTimeflag;
        }
        return resMap;
    }

    /**
     * 将map阶段传递过来的数据按照unixtime从小到大排序(使用TreeMap)
     *
     * @param context reducer的context上下文,用于设置counter
     * @param values  map阶段传递过来的数据
     * @return key为unixtime, value为pos
     */
    public static TreeMap<Long, String> getSortedData(Reducer.Context context, Iterable<Text> values) {
        TreeMap<Long, String> sortedData = new TreeMap<Long, String>();
        for (Text v : values) {
            String[] vs = v.toString().split(",");
            try {
                sortedData.put(Long.parseLong(vs[1]), vs[0]);
            } catch (NumberFormatException num) {
                context.getCounter(Counter.TIMESKIP).increment(1);
            }
        }
        return sortedData;
    }
}
