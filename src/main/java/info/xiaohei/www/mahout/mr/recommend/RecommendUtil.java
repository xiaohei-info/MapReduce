package info.xiaohei.www.mahout.mr.recommend;

import info.xiaohei.www.mahout.enums.EvaluatorType;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.*;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.util.List;

/**
 * Created by xiaohei on 16/2/28.
 */
public class RecommendUtil {

    final static int RECOMMENDER_NUM = 3;
    final static int NEIGHBORHOOD_NUM = 2;

    /**
     * 根据用户id打印出给该用户的推荐
     *
     * @param uid              用户id
     * @param recommendedItems 推荐列表
     * @param isSkip           是否跳过
     */
    public static void showRecommendResult(long uid, List<RecommendedItem> recommendedItems, boolean isSkip) {
        if (!isSkip || recommendedItems.size() > 0) {
            System.out.printf("uid:%s,", uid);
            for (RecommendedItem recommendedItem : recommendedItems) {
                System.out.printf("(%s,%f)", recommendedItem.getItemID(), recommendedItem.getValue());
            }
            System.out.println();
        }
    }

    /**
     * 打印出算法的评分
     */
    public static void evaluate(EvaluatorType type, RecommenderBuilder recommenderBuilder
            , DataModelBuilder dataModelBuilder, DataModel dataModel, double trainPt) throws TasteException {
        System.out.printf("%s Evaluater Score:%s\n", type.toString()
                , RecommendFactory.getRecommenderEvaluator(type).evaluate(recommenderBuilder
                        , dataModelBuilder, dataModel, trainPt, 1.0));
    }

    /**
     * 打印出算法的评分
     */
    public static void evaluate(RecommenderEvaluator recommenderEvaluator, RecommenderBuilder recommenderBuilder
            , DataModelBuilder dataModelBuilder, DataModel dataModel, double trainPt) throws TasteException {
        System.out.printf("Evaluater Score:%s\n", recommenderEvaluator.evaluate(recommenderBuilder
                , dataModelBuilder, dataModel, trainPt, 1.0));
    }

    /**
     * 统计算法评分
     */
    public static void statsEvaluator(RecommenderBuilder recommenderBuilder, DataModelBuilder dataModelBuilder
            , DataModel dataModel, int topn) throws TasteException {
        RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
        IRStatistics stats = evaluator.evaluate(recommenderBuilder, dataModelBuilder, dataModel, null, topn
                , GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
        System.out.printf("Recommender IR Evaluator: [Precision:%s,Recall:%s]\n", stats.getPrecision(), stats.getRecall());
    }
}
