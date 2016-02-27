package info.xiaohei.www.mahout.mr.test;

import info.xiaohei.www.HadoopUtil;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

/**
 * Created by xiaohei on 16/2/27.
 */
public class ItermCf {
    public static void main(String[] args) throws Exception {
        String inPath = HadoopUtil.HDFS + "/data/4-mahout/iterm.csv";
        String outPath = HadoopUtil.HDFS + "out/4-mahout";

        String sb = "--input " + inPath +
                " --output " + outPath +
                " --booleanData true" +
                " --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity";
        //sb.append(" --tempDir ").append(tmpPath);
        args = sb.split(" ");

        RecommenderJob job = new RecommenderJob();
        job.run(args);
    }
}
