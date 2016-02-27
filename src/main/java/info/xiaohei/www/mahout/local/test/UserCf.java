package info.xiaohei.www.mahout.local.test;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by xiaohei on 16/2/27.
 */
public class UserCf {
    public static void main(String[] args) throws IOException, TasteException {
        DataModel dataModel = new FileDataModel(new File("/Users/xiaohei/Downloads/maven_mahout_template-mahout_recommend_v1" +
                "/datafile/item.csv"));
        UserSimilarity userSimilarity = new EuclideanDistanceSimilarity(dataModel);
        UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(4, userSimilarity, dataModel);
        Recommender recommender = new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);
        LongPrimitiveIterator iterator = dataModel.getUserIDs();
        while (iterator.hasNext()) {
            long uid = iterator.nextLong();
            List<RecommendedItem> recommendedItems = recommender.recommend(uid, 4);
            System.out.printf("uid:%s", uid);
            for (RecommendedItem recommendedItem : recommendedItems) {
                System.out.printf("(%s,%f)", recommendedItem.getItemID(), recommendedItem.getValue());
            }
            System.out.println();
        }
    }
}
