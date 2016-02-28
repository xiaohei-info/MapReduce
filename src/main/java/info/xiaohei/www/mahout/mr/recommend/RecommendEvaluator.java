package info.xiaohei.www.mahout.mr.recommend;

import info.xiaohei.www.mahout.enums.EvaluatorType;
import info.xiaohei.www.mahout.enums.NeighborhoodType;
import info.xiaohei.www.mahout.enums.SimilarityType;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/27.
 * 针对各个算法组合进行评估
 */
public class RecommendEvaluator {

    public static void main(String[] args) throws IOException, TasteException {
        String filePath = "/Users/xiaohei/Downloads/datafile/item.csv";
        DataModel dataModel = RecommendFactory.getDataModel(filePath);
        userLoglikelihood(dataModel);
        userCityBlock(dataModel);
        userTanimoto(dataModel);
        itemLoglikelihood(dataModel);
        itemCityBlock(dataModel);
        itemTanimoto(dataModel);
        svd(dataModel);
    }

    /**
     * 对数似然相似度+最近距离邻居+usercf
     */
    public static RecommenderBuilder userLoglikelihood(DataModel dataModel) throws TasteException {
        System.out.println("userLoglikelihood");
        UserSimilarity userSimilarity = RecommendFactory.getUserSimilarity(SimilarityType.LOGLIKELIHOOD, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.getUserNeighborhood(NeighborhoodType.NEAREST,
                RecommendUtil.NEIGHBORHOOD_NUM, userSimilarity, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.getUserRecommenderBuidler(false, userSimilarity, userNeighborhood);
        RecommendUtil.evaluate(EvaluatorType.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendUtil.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    /**
     * 曼哈顿距离+最近距离邻居+usercf
     */
    public static RecommenderBuilder userCityBlock(DataModel dataModel) throws TasteException {
        System.out.println("userCityBlock");
        UserSimilarity userSimilarity = RecommendFactory.getUserSimilarity(SimilarityType.CITYBLOCK, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.getUserNeighborhood(NeighborhoodType.NEAREST
                , RecommendUtil.NEIGHBORHOOD_NUM, userSimilarity, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.getUserRecommenderBuidler(false, userSimilarity
                , userNeighborhood);
        RecommendUtil.evaluate(EvaluatorType.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null
                , dataModel, 0.7);
        RecommendUtil.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    /**
     * Tanimoto相似度+最近距离邻居+usercf
     */
    public static RecommenderBuilder userTanimoto(DataModel dataModel) throws TasteException, IOException {
        System.out.println("userTanimoto");
        UserSimilarity userSimilarity = RecommendFactory.getUserSimilarity(SimilarityType.TANIMOTO, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.getUserNeighborhood(NeighborhoodType.NEAREST,
                RecommendUtil.NEIGHBORHOOD_NUM, userSimilarity, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.getUserRecommenderBuidler(false, userSimilarity
                , userNeighborhood);

        RecommendUtil.evaluate(EvaluatorType.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendUtil.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    /**
     * 对数似然相似度+itemcf
     */
    public static RecommenderBuilder itemLoglikelihood(DataModel dataModel) throws TasteException {
        System.out.println("itemLoglikelihood");
        ItemSimilarity itemSimilarity = RecommendFactory.getItemSimilarity(SimilarityType.LOGLIKELIHOOD, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.getItemRecommenderBuidler(false, itemSimilarity);
        RecommendUtil.evaluate(EvaluatorType.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendUtil.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    /**
     * 曼哈顿距离+itermcf
     */
    public static RecommenderBuilder itemCityBlock(DataModel dataModel) throws TasteException {
        System.out.println("itemCityBlock");
        ItemSimilarity itemSimilarity = RecommendFactory.getItemSimilarity(SimilarityType.CITYBLOCK, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.getItemRecommenderBuidler(false, itemSimilarity);
        RecommendUtil.evaluate(EvaluatorType.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendUtil.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    /**
     * tanimoto相似度+itemcf
     */
    public static RecommenderBuilder itemTanimoto(DataModel dataModel) throws TasteException, IOException {
        System.out.println("itemTanimoto");
        ItemSimilarity itemSimilarity = RecommendFactory.getItemSimilarity(SimilarityType.TANIMOTO, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.getItemRecommenderBuidler(false, itemSimilarity);
        RecommendUtil.evaluate(EvaluatorType.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendUtil.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }

    /**
     * svd推荐算法
     */
    public static RecommenderBuilder svd(DataModel dataModel) throws TasteException {
        System.out.println("svd");
        RecommenderBuilder recommenderBuilder = RecommendFactory.getSVDRecommenderBuidler(new ALSWRFactorizer(dataModel, 5, 0.05, 10));
        RecommendUtil.evaluate(EvaluatorType.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        RecommendUtil.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        return recommenderBuilder;
    }
}
