package info.xiaohei.www.mr.recommend.sort;

import info.xiaohei.www.HadoopUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/26.
 * 对推荐结果进排序
 */
public class SortMapper extends Mapper<LongWritable, Text, SortData, NullWritable> {
    SortData sortData = new SortData();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
        sortData.setUserId(strArr[0]);
        sortData.setItermId(strArr[1].split(":")[0]);
        sortData.setPerference(Double.valueOf(strArr[1].split(":")[1]));
        context.write(sortData, NullWritable.get());
    }
}
