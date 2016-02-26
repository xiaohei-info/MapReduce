package info.xiaohei.www.mr.recommend.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xiaohei on 16/2/26.
 * 对应每行推荐结果的值,重写排序方法,按得分的从高到低进行排序
 */
public class SortData implements WritableComparable<SortData> {

    private String userId;
    private String itermId;
    private Double perference;

    public SortData() {
    }

    public SortData(String userId, String itermId, Double perference) {
        this.userId = userId;
        this.itermId = itermId;
        this.perference = perference;
    }

    public int compareTo(SortData o) {
        if (this.userId.equals(o.getUserId())) {
            return (int) (o.perference - this.perference);
        } else {
            return Integer.parseInt(this.userId) - Integer.parseInt(o.getUserId());
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.itermId);
        dataOutput.writeDouble(this.perference);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readUTF();
        this.itermId = dataInput.readUTF();
        this.perference = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return this.userId + "\t" + this.itermId + ":" + this.perference;
    }

    @Override
    public int hashCode() {
        return this.userId.hashCode() + this.itermId.hashCode() + this.perference.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SortData)) {
            return false;
        }
        SortData sortData = (SortData) obj;
        return (this.userId.equals(sortData.userId)
                && this.itermId.equals(sortData.itermId)
                && this.perference.equals(sortData.perference));
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItermId() {
        return itermId;
    }

    public void setItermId(String itermId) {
        this.itermId = itermId;
    }

    public Double getPerference() {
        return perference;
    }

    public void setPerference(Double perference) {
        this.perference = perference;
    }
}
