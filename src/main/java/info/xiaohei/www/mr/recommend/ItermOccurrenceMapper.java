package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.HadoopUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/24.
 * 计算物品同现矩阵的map过程,任务是将用户评分矩阵数据转换为物品与物品之间的全排列组合输出
 */
public class ItermOccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //输入的数据格式为:1	103:2.5,101:5.0,102:3.0
        String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
        //提取每行的itermId进行全排列输出
        for (int i = 1; i < strArr.length; i++) {
            String itermId1 = strArr[i].split(":")[0];
            for (int j = 1; j < strArr.length; j++) {
                String itermId2 = strArr[j].split(":")[0];
                k.set(itermId1 + ":" + itermId2);
                context.write(k, one);
            }
        }
    }
}
