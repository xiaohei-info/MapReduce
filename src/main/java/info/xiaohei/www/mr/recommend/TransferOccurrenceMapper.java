package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.mr.Util;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/25.
 * 将物品同现矩阵进行转换
 */
public class TransferOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = Util.SPARATOR.split(value.toString());
        String itermId1 = strArr[0].split(":")[0];
        String itermId2 = strArr[0].split(":")[1];
        String occurrence = strArr[1];
        k.set(itermId1);
        v.set(itermId2 + ":" + occurrence);
        context.write(k, v);
    }
}
