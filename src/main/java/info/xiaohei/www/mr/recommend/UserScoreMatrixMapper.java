package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.HadoopUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/24.
 * 将原始数据进行转换,以每行UserId为key,ItermId:Perference作为value输出
 */
public class UserScoreMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text userId = new Text();
    Text itermAndPer = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //输入的数据格式为:1,101,5.0
        String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
        userId.set(strArr[0]);
        itermAndPer.set(strArr[1] + ":" + strArr[2]);
        context.write(userId, itermAndPer);
    }
}
