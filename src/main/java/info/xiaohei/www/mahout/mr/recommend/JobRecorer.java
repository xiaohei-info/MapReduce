package info.xiaohei.www.mahout.mr.recommend;

import org.apache.mahout.cf.taste.recommender.IDRescorer;

import java.util.Set;

/**
 * Created by xiaohei on 16/2/28.
 */
public class JobRecorer implements IDRescorer {

    private Set<Long> jobIds;

    public JobRecorer(Set<Long> jobIds) {
        this.jobIds = jobIds;
    }

    public double rescore(long l, double v) {
        return isFiltered(l) ? Double.NaN : v;
    }

    public boolean isFiltered(long l) {
        return jobIds.contains(l);
    }
}
