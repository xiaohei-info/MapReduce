package info.xiaohei.www.mr.kpi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 *
 * 将kpi的统计结果进行排序的mapper
 */
public class SortMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, SortKey, NullWritable> {

    //使用自定义的类型作为key
    SortKey sortKey = new SortKey();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = value.toString().split("\t");
        sortKey.setFirst(strArr[0]);
        sortKey.setSecond(Integer.parseInt(strArr[1]));
        context.write(sortKey, NullWritable.get());
    }
}
