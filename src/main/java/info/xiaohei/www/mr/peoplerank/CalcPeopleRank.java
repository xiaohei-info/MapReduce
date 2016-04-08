package info.xiaohei.www.mr.peoplerank;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.HdfsUtil;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiaohei on 16/3/9.
 * 将邻接概率矩阵和pr矩阵进行计算并将得到的pr结果输出
 */
public class CalcPeopleRank {

    /**
     * 输入邻接概率矩阵和pr矩阵
     * 按照矩阵相乘的公式,将对应的数据输出到reduce进行计算
     */
    public static class CalcPeopleRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();
        String flag = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            flag = fileSplit.getPath().getName();
            System.out.println("CalcPeopleRankMapper input type:");
            System.out.println(flag);
        }

        /**
         * k的作用是将pr矩阵的列和邻接矩阵的行对应起来
         * 如:pr矩阵的第一列要和邻接矩阵的第一行相乘,所以需要同时输入到reduce中
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            int nums = 25;
            //处理pr矩阵
            if (flag.startsWith("peoplerank")) {
                String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
                //第一位为用户id,输入的每行内容都为pr矩阵中的一列,所以也可以看成是列数
                k.set(strArr[0]);
                for (int i = 1; i <= nums; i++) {
                    //pr为标识符,i为该列中第i行,strArr[1]为值
                    v.set("pr:" + i + "," + strArr[1]);
                    context.write(k, v);
                }
            }
            //处理邻接概率矩阵
            else {
                String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
                //k为用户id,输入的每行就是邻接概率矩阵中的一行,所以也可以看成行号
                k.set(strArr[0]);
                for (int i = 1; i < strArr.length; i++) {
                    //matrix为标识符,i为该行中第i列,strArr[i]为值
                    v.set("matrix:" + i + "," + strArr[i]);
                    context.write(k, v);
                }
            }
        }
    }

    /**
     * 每行输入都是两个矩阵相乘中对应的值
     * 如:邻接矩阵的第一行的值和pr矩阵第一列的值
     */
    public static class CalcPeopleRankReducer extends Reducer<Text, Text, Text, Text> {

        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("CalcPeopleRankReducer input:");
            StringBuilder printStr = new StringBuilder();
            //pr统计
            float pr = 0f;
            //存储pr矩阵列的值
            Map<Integer, Float> prMap = new HashMap<Integer, Float>();
            //存储邻接矩阵行的值
            Map<Integer, Float> matrixMap = new HashMap<Integer, Float>();
            //将两个矩阵对应的值存入对应的map中
            for (Text value : values) {
                String valueStr = value.toString();
                String[] kv = HadoopUtil.SPARATOR.split(valueStr.split(":")[1]);
                if (valueStr.startsWith("pr")) {
                    prMap.put(Integer.parseInt(kv[0]), Float.valueOf(kv[1]));
                } else {
                    matrixMap.put(Integer.parseInt(kv[0]), Float.valueOf(kv[1]));
                }
                printStr.append(",").append(valueStr);
            }
            System.out.println(printStr.toString().replaceFirst(",", ""));
            //根据map中的数据进行计算
            for (Map.Entry<Integer, Float> entry : matrixMap.entrySet()) {
                pr += entry.getValue() * prMap.get(entry.getKey());
            }
            v.set(String.valueOf(pr));
            System.out.println("CalcPeopleRankReducer output:");
            System.out.println(key.toString() + ":" + v.toString());
            System.out.println();
            context.write(key, v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inPath1 = HadoopUtil.HDFS + "/out/5-peoplerank/probility-matrix";
        String inPath2 = HadoopUtil.HDFS + "/data/5-peoplerank/peoplerank.csv";
        String outPath = HadoopUtil.HDFS + "/out/5-peoplerank/pr";
        JobInitModel job = new JobInitModel(new String[]{inPath1, inPath2}, outPath, conf, null, "CalcPeopleRank", CalcPeopleRank.class
                , null, CalcPeopleRankMapper.class, Text.class, Text.class, null, null
                , CalcPeopleRankReducer.class, Text.class, Text.class);
        BaseDriver.initJob(new JobInitModel[]{job});


        HdfsUtil.rmr(inPath2);
        HdfsUtil.rename(outPath + "/part-r-00000", inPath2);
    }
}
