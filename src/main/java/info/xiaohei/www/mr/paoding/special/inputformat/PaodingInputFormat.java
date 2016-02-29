package info.xiaohei.www.mr.paoding.special.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/29.
 * 自定义的分片类,继承CombineFileInputFormat可以将多个小文件进行合并,泛型参数为输入map函数中的key,value类型
 */
public class PaodingInputFormat extends CombineFileInputFormat<Text, Text> {

    /**
     * 重写次方法,直接放回false,对所有文件都不进行切割,保持完整
     */
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    /**
     * 重写此方法,返回的CombineFileRecordReader为处理每个分片的recordReader,在构造函数中设置自定义的RecordReader对象
     */
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new CombineFileRecordReader<Text, Text>((CombineFileSplit) inputSplit, taskAttemptContext, PaodingRecordReader.class);
    }
}
