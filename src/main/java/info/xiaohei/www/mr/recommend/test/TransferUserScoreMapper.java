package info.xiaohei.www.mr.recommend.test;

import info.xiaohei.www.HadoopUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/25.
 * 将用户评分矩阵转换为itermId userId:perference的形式
 */
public class TransferUserScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
        for (int i = 1; i < strArr.length; i++) {
            k.set(strArr[i].split(":")[0]);
            v.set(strArr[0] + ":" + strArr[i].split(":")[1]);
            context.write(k, v);
        }
    }
}
