package info.xiaohei.www.mahout.mr.recommend;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.io.IOException;
import java.util.List;

/**
 * Created by xiaohei on 16/2/28.
 */
public class RecommendResult {

    public static void main(String[] args) throws IOException, TasteException {
        String filePath = "/Users/xiaohei/Downloads/datafile/job/pv.csv";
        DataModel dataModel = RecommendFactory.getDataModel(filePath);
        RecommenderBuilder rb1 = RecommendEvaluator.userCityBlock(dataModel);
        RecommenderBuilder rb2 = RecommendEvaluator.itemLoglikelihood(dataModel);
        LongPrimitiveIterator iterator = dataModel.getUserIDs();
        while (iterator.hasNext()) {
            long uid = iterator.nextLong();
            System.out.print("userCityBlock=>");
            result(uid, rb1, dataModel);
            System.out.print("itemLoglikelihood=>");
            result(uid, rb2, dataModel);
        }
    }

    public static void result(long uid, RecommenderBuilder rb, DataModel dataModel) throws TasteException {
        List<RecommendedItem> recommendedItemList = rb.buildRecommender(dataModel).recommend(uid, RecommendUtil.RECOMMENDER_NUM);
        RecommendUtil.showRecommendResult(uid, recommendedItemList, false);
    }
}
