package info.xiaohei.www.mr.paoding.special;

import net.paoding.analysis.analyzer.PaodingAnalyzer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by xiaohei on 16/2/29.
 * 输入的key为类别名,value为整个文件内容
 */
public class PaodingMapper extends Mapper<Text, Text, Text, Text> {
    Text v = new Text();

    PaodingAnalyzer analyzer = new PaodingAnalyzer();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        StringBuilder totalStr = new StringBuilder();
        //对文件内容进行分词
        TokenStream tokenStream = analyzer.tokenStream("", new StringReader(value.toString()));
        while (tokenStream.incrementToken()) {
            CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
            totalStr.append(attribute.toString()).append(" ");
        }
        v.set(totalStr.toString());
        context.write(key, v);
    }
}
