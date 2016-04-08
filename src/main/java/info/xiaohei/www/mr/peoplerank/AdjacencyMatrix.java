package info.xiaohei.www.mr.peoplerank;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by xiaohei on 16/3/9.
 * 将用户原始数据集转换成邻接表->邻接矩阵->邻接概率矩阵的过程
 */
public class AdjacencyMatrix {

    /**
     * 输出邻接表
     */
    public static class AdjacencyMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            System.out.println("AdjacencyMapper input:");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //打印当前读入的数据
            System.out.println(value.toString());
            String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
            //原始用户id为key,目标用户id为value
            k.set(strArr[0]);
            v.set(strArr[1]);
            context.write(k, v);
        }
    }

    /**
     * 输入邻接表
     * 输出邻接概率矩阵
     * 邻接矩阵*阻尼系数/该用户链出数+概率矩阵=邻接概率矩阵
     */
    public static class AdjacencyReducer extends Reducer<Text, Text, Text, Text> {

        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //初始化概率矩阵,概率矩阵只有一列,函数和总用户数相同
            //用户数
            int nums = 25;
            float[] G = new float[nums];
            //概率矩阵的值为pr公式的(1-d)/n的部分
            //阻尼系数
            float d = 0.85f;
            Arrays.fill(G, (1 - d) / nums);
            //构建用户邻接矩阵
            float[] U = new float[nums];
            //该用户的链出数
            int out = 0;
            StringBuilder printSb = new StringBuilder();
            for (Text value : values) {
                //从value中拿到目标用户的id
                int targetUserIndex = Integer.parseInt(value.toString());
                //邻接矩阵中每个目标用户对应的值为1,其余为0
                U[targetUserIndex - 1] = 1;
                out++;
                printSb.append(",").append(value.toString());
            }
            //打印reducer的输入
            System.out.println("AdjacencyReducer input:");
            System.out.println(key.toString() + ":" + printSb.toString().replaceFirst(",", ""));

            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < nums; i++) {
                stringBuilder.append(",").append(G[i] + U[i] * d / out);
            }
            v.set(stringBuilder.toString().replaceFirst(",", ""));
            System.out.println("AdjacencyReducer output:");
            System.out.println(key.toString() + ":" + v.toString());
            System.out.println();
            context.write(key, v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath = HadoopUtil.HDFS + "/data/5-peoplerank/people.csv";
        String outPath = HadoopUtil.HDFS + "/out/5-peoplerank/probility-matrix";
        JobInitModel job = new JobInitModel(new String[]{inPath}, outPath, conf, null, "AdjacencyMatrix", AdjacencyMatrix.class
                , null, AdjacencyMapper.class, Text.class, Text.class, null, null
                , AdjacencyReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});
    }
}
