package info.xiaohei.www.mr.paoding.special;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import info.xiaohei.www.mr.paoding.special.inputformat.PaodingInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/29.
 */
public class PaodingDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        //设置一个分片的最大大小
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 4000000);
        //获得该目录下的所有子目录作为输入
        /*String inPath = HadoopUtil.HDFS + "/data/4-paoding/data";
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(inPath));
        String[] inPaths = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            inPaths[i] = fileStatuses[i].getPath().getName();
        }*/


        //以上方式突然有异常,换成下面这种
        String inPath = HadoopUtil.HDFS + "/data/4-paoding/data";
        String outPath = HadoopUtil.HDFS + "/out/4-paoding/special";
        //设置自定义的PaodingInputFormat
        JobInitModel job = new JobInitModel(new String[]{inPath + "/MP3", inPath + "/camera", inPath + "/computer"
                , inPath + "/household", inPath + "/mobile"}, outPath, conf, null, "paoding-special", PaodingDriver.class
                , PaodingInputFormat.class, PaodingMapper.class, Text.class, Text.class, null, null
                , null, null, null);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
