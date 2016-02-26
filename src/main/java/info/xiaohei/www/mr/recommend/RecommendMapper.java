package info.xiaohei.www.mr.recommend;

import info.xiaohei.www.mr.HadoopUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiaohei on 16/2/24.
 * 根据同现度矩阵和用户评分矩阵计算推荐结果
 */
public class RecommendMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    Text k = new Text();
    DoubleWritable v = new DoubleWritable();
    //第二个Map存储的是同现矩阵列方向上的itermId和对应的同现度
    Map<String, Map<String, Double>> colItermOccurrenceMap = new HashMap<String, Map<String, Double>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            String path = context.getLocalCacheFiles()[0].getName();
            File itermOccurrenceMatrix = new File(path);
            FileReader fileReader = new FileReader(itermOccurrenceMatrix);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String s;
            while ((s = bufferedReader.readLine()) != null) {
                String[] strArr = HadoopUtil.SPARATOR.split(s);
                String[] firstStr = strArr[0].split(":");
                String itermId1 = firstStr[0];
                String itermId2 = firstStr[1];
                Double perference = Double.parseDouble(strArr[1]);
                Map<String, Double> colItermMap;
                if (!colItermOccurrenceMap.containsKey(itermId1)) {
                    colItermMap = new HashMap<String, Double>();
                } else {
                    colItermMap = colItermOccurrenceMap.get(itermId1);
                }
                colItermMap.put(itermId2, perference);
                colItermOccurrenceMap.put(itermId1, colItermMap);
            }
            bufferedReader.close();
            fileReader.close();
        }
        for (Map.Entry<String, Map<String, Double>> entry : colItermOccurrenceMap.entrySet()) {
            System.out.println(entry.getKey());
            for (Map.Entry<String, Double> entry1 : entry.getValue().entrySet()) {
                System.out.println(entry1.getKey() + ":" + entry1.getValue());
            }
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = HadoopUtil.SPARATOR.split(value.toString());
        String[] firstStr = strArr[0].split(":");

        String userId = firstStr[0];
        //循环物品同现矩阵的行
        for (Map.Entry<String, Map<String, Double>> rowEntry : colItermOccurrenceMap.entrySet()) {
            //要计算用户对其喜好度的itermId
            String targetItermId = rowEntry.getKey();
            //计算得到的总得分
            Double totalScore = 0.0;
            //存储着该targetItermId对应同现矩阵上的一行
            Map<String, Double> colIterMap = rowEntry.getValue();

            for (int i = 1; i < strArr.length; i++) {
                String[] itermPer = strArr[i].split(":");
                //同现矩阵上列方向的ItermId
                String itermId2 = itermPer[0];
                Double perference = Double.parseDouble(itermPer[1]);

                Double occurrence = 0.0;
                if (colIterMap.get(itermId2) != null) {
                    occurrence = colIterMap.get(itermId2);
                }
                Double score = perference * occurrence;
                totalScore += score;
            }
            k.set(userId + ":" + targetItermId);
            v.set(totalScore);
            context.write(k, v);
        }
    }
}
