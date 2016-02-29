package info.xiaohei.www.mr.paoding.normal;

import net.paoding.analysis.analyzer.PaodingAnalyzer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by xiaohei on 16/2/29.
 * 使用庖丁分词对输入的每一行进行切分,输出的key为列表,value为该行的分词结果
 */
public class PaodingMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    //庖丁分词器
    PaodingAnalyzer analyzer = new PaodingAnalyzer();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringBuilder totalStr = new StringBuilder();
        //对读取的行进行分词并拼接
        TokenStream tokenStream = analyzer.tokenStream("", new StringReader(value.toString()));
        while (tokenStream.incrementToken()) {
            CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
            totalStr.append(attribute.toString()).append(" ");
        }
        //获取文件父目录名,即为类别名
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String category = fileSplit.getPath().getParent().getName();
        k.set(category);
        v.set(totalStr.toString());
        context.write(k, v);
    }
}
