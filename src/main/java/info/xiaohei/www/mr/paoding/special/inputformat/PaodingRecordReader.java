package info.xiaohei.www.mr.paoding.special.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/29.
 * 自定义的RecordReader类,用来处理CombineFileInputSplit返回的每个分片
 */
public class PaodingRecordReader extends RecordReader<Text, Text> {

    private CombineFileSplit combineFileSplit;//当前处理的分片
    private Configuration conf;//系统信息
    private int currentIndex;//当前处理到第几个分片

    private Text currentKey = new Text();//当前key
    private Text currentValue = new Text();//当前value
    private boolean isReaded = false;//是否已经读取过了该分片
    private float currentProgress = 0;//当前读取进度

    private FSDataInputStream inputStream;//HDFS文件流读取

    /**
     * 构造函数必须的三个参数,自定义的InputFormat类每次读取新的分片时,都会实例化自定义的RecordReader类对象来对其进行读取
     *
     * @param combineFileSplit   当前读取的分片
     * @param taskAttemptContext 系统上下文环境
     * @param index              当前分片中处理的文件索引
     */
    public PaodingRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext taskAttemptContext, Integer index) {
        this.combineFileSplit = combineFileSplit;
        this.conf = taskAttemptContext.getConfiguration();
        this.currentIndex = index;
    }

    /**
     * 初始化RecordReader的一些设置
     * */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }


    /**
     * 返回true就取出key和value,之后index前移,返回false就结束循环表示没有文件内容可读取了
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //没被读取过的文件才进行读取
        if (!this.isReaded) {
            //根据当前的文件索引从当前分片中找到对应的文件路径
            Path path = this.combineFileSplit.getPath(this.currentIndex);
            //获取父目录名即为类别名
            this.currentKey.set(path.getParent().getName());
            //从当前分片中获得当前文件的长度
            byte[] content = new byte[(int) this.combineFileSplit.getLength(this.currentIndex)];
            try {
                //读取该文件内容
                FileSystem fs = path.getFileSystem(this.conf);
                this.inputStream = fs.open(path);
                this.inputStream.readFully(content);
            } catch (Exception ignored) {
            } finally {
                assert inputStream != null;
                inputStream.close();
            }
            this.currentValue.set(content);
            this.isReaded = true;
            return true;
        }
        return false;
    }

    /**
     * 返回当前key的方法
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.currentKey;
    }

    /**
     * 返回当前value的方法
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.currentValue;
    }

    /**
     * 返回当前的处理进度
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        //获得当前分片中的总文件数
        int splitFileNum = this.combineFileSplit.getPaths().length;
        if (this.currentIndex >= 0 && this.currentIndex < splitFileNum) {
            //当前处理的文件索引除以文件总数得到处理的进度
            this.currentProgress = (float) this.currentIndex / splitFileNum;
            return this.currentProgress;
        }
        return this.currentProgress;
    }

    @Override
    public void close() throws IOException {
        if (this.inputStream != null) {
            this.inputStream.close();
        }
    }
}
