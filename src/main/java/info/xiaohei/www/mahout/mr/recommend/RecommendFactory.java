package info.xiaohei.www.mahout.mr.recommend;

import info.xiaohei.www.mahout.enums.EvaluatorType;
import info.xiaohei.www.mahout.enums.NeighborhoodType;
import info.xiaohei.www.mahout.enums.SimilarityType;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;

/**
 * Created by xiaohei on 16/2/27.
 * 推荐相关信息获取的工厂类
 */
public final class RecommendFactory {

    /**
     * 根据文件路径读取有评分的数据
     *
     * @param filePath 文件路径
     * @return 封装好的DataModel
     */
    public static DataModel getDataModel(String filePath) throws IOException {
        return new FileDataModel(new File(filePath));
    }

    /**
     * 根据文件路径读取 没有 评分的数据
     *
     * @param filePath 文件路径
     * @return 返回的DataModel中不包含评分数据
     */
    public static DataModel getBooleanPerDataModel(String filePath) throws IOException, TasteException {
        return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(new FileDataModel(new File(filePath))));
    }

    /**
     * 获得DatamodelBuilder
     * */
    public static DataModelBuilder getDataModelBuilder() {
        return new DataModelBuilder() {
            public DataModel buildDataModel(FastByIDMap<PreferenceArray> fastByIDMap) {
                return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(fastByIDMap));
            }
        };
    }

    /**
     * 根据相似度类型和数据返回计算得到的相似度对象
     *
     * @param similarityType 相似度类型
     * @param dataModeld     数据
     * @return 对应的相似度对象实例
     */
    public static UserSimilarity getUserSimilarity(SimilarityType similarityType, DataModel dataModeld) throws TasteException {
        switch (similarityType) {
            case PEARSON:
                return new PearsonCorrelationSimilarity(dataModeld);
            case EUCLIDEAN:
                return new EuclideanDistanceSimilarity(dataModeld);
            case COSINE:
                return new UncenteredCosineSimilarity(dataModeld);
            case TANIMOTO:
                return new TanimotoCoefficientSimilarity(dataModeld);
            case LOGLIKELIHOOD:
                return new LogLikelihoodSimilarity(dataModeld);
            case SPEARMAN:
                return new SpearmanCorrelationSimilarity(dataModeld);
            case CITYBLOCK:
                return new CityBlockSimilarity(dataModeld);
            default:
                return new EuclideanDistanceSimilarity(dataModeld);
        }
    }

    /**
     * 根据相似度类型和数据返回计算得到的相似度对象
     *
     * @param similarityType 相似度类型
     * @param dataModeld     数据
     * @return 对应的相似度对象实例
     */
    public static ItemSimilarity getItemSimilarity(SimilarityType similarityType, DataModel dataModeld) throws TasteException {
        switch (similarityType) {
            case PEARSON:
                return new PearsonCorrelationSimilarity(dataModeld);
            case EUCLIDEAN:
                return new EuclideanDistanceSimilarity(dataModeld);
            case COSINE:
                return new UncenteredCosineSimilarity(dataModeld);
            case TANIMOTO:
                return new TanimotoCoefficientSimilarity(dataModeld);
            case LOGLIKELIHOOD:
                return new LogLikelihoodSimilarity(dataModeld);
            case CITYBLOCK:
                return new CityBlockSimilarity(dataModeld);
            default:
                return new EuclideanDistanceSimilarity(dataModeld);
        }
    }

    /**
     * 根据计算邻居的类型返回对应的用户邻居对象
     *
     * @param neighborhoodType 计算方式
     * @param neighborhoodNums 多少个邻居
     * @param userSimilarity   用户相似度
     * @param dataModel        计算数据
     * @return 用户的邻居
     */
    public static UserNeighborhood getUserNeighborhood(NeighborhoodType neighborhoodType, double neighborhoodNums
            , UserSimilarity userSimilarity, DataModel dataModel) throws TasteException {
        switch (neighborhoodType) {
            case NEAREST:
                return new NearestNUserNeighborhood((int) neighborhoodNums, userSimilarity, dataModel);
            case THRESHOLD:
            default:
                return new ThresholdUserNeighborhood(neighborhoodNums, userSimilarity, dataModel);
        }
    }

    /**
     * 根据数据,用户相似度,用户邻居得到推荐器
     *
     * @param isPref           数据是否有评分数据
     * @param userSimilarity   用户相似度
     * @param userNeighborhood 用户邻居
     * @return 对应的推荐器
     */
    public static RecommenderBuilder getUserRecommenderBuidler(Boolean isPref, final UserSimilarity userSimilarity
            , final UserNeighborhood userNeighborhood) {
        return isPref ? new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                return new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);
            }
        }
                : new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                return new GenericBooleanPrefUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);
            }
        };
    }

    /**
     * 根据数据,物品相似度得到推荐器
     *
     * @param isPref         数据是否有评分数据
     * @param itemSimilarity 物品相似度
     * @return 对应的推荐器
     */
    public static RecommenderBuilder getItemRecommenderBuidler(Boolean isPref, final ItemSimilarity itemSimilarity) {
        return isPref ? new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                return new GenericItemBasedRecommender(dataModel, itemSimilarity);
            }
        } : new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                return new GenericBooleanPrefItemBasedRecommender(dataModel, itemSimilarity);
            }
        };
    }

    /**
     * 得到SVD推荐器
     *
     * @param factorizer 参数
     * @return 推荐器
     */
    public static RecommenderBuilder getSVDRecommenderBuidler(final Factorizer factorizer) throws TasteException {
        return new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                return new SVDRecommender(dataModel, factorizer);
            }
        };
    }

    /**
     * 获得对应的评估器
     *
     * @param evaluatorType 评估器类型
     * @return 算法评估器
     */
    public static RecommenderEvaluator getRecommenderEvaluator(EvaluatorType evaluatorType) {
        switch (evaluatorType) {
            case AVERAGE_ABSOLUTE_DIFFERENCE:
                return new AverageAbsoluteDifferenceRecommenderEvaluator();
            case RMS:
            default:
                return new RMSRecommenderEvaluator();
        }
    }
}
