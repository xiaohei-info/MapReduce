package info.xiaohei.www.mr.paoding.normal;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/29.
 * 使用默认的TextInputFormat对输入文件进行分片
 */
public class PaodingDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //获得该目录下的所有子目录作为输入
        String inPath = HadoopUtil.HDFS + "/data/4-paoding/data";
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(inPath));
        String[] inPaths = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            inPaths[i] = fileStatuses[i].getPath().getName();
        }
        String outPath = HadoopUtil.HDFS + "/out/4-paoding/normal";
        JobInitModel job = new JobInitModel(inPaths, outPath, conf, null, "paoding-normal", PaodingDriver.class
                , null, PaodingMapper.class, Text.class, Text.class, null, null, PaodingReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
