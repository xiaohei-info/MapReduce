package info.xiaohei.www.mr.paoding;

import net.paoding.analysis.analyzer.PaodingAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by xiaohei on 16/2/28.
 * 本地测试庖丁分词
 */
public class Test {
    public static void main(String[] args) throws IOException {
        String line = "我叫小黑,我是一名程序猿,很喜欢看电影!";
        //构建庖丁分词对象
        PaodingAnalyzer analyzer = new PaodingAnalyzer();
        //读取要解析的字符串
        StringReader reader = new StringReader(line);
        //tokenStream获得一个分词流
        TokenStream tokenStream = analyzer.tokenStream("", reader);
        //每调用一次incrementToken都会执行一次分词,当没有词可以分的时候返回false
        while (tokenStream.incrementToken()) {
            //getAttribute获得当前分词的结果,一般使用CharTermAttribute接收
            CharTermAttribute charTermAttribute = tokenStream.getAttribute(CharTermAttribute.class);
            System.out.println(charTermAttribute.toString());
        }
    }
}
