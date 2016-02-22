package info.xiaohei.www.mr.kpi;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 *
 * 排序任务mr中自定义的key类型,作用为将所有结果按照kpi的数值从大到小排序
 * 此类必须实现WritableComparable泛型接口,泛型类为该类本身
 * 重写了comparaTo,write,readFields,hashCode,toString和equals方法
 */
public class SortKey implements WritableComparable<SortKey> {

    private String first;//kpi名称,如统计各个时间的kpi的话,该值就是这段时间
    private Integer second;//kpi的数值

    //提供的构造函数
    public SortKey() {
    }

    public SortKey(String first, Integer second) {
        this.first = first;
        this.second = second;
    }

    /**
     * 该方法在map过程之后的排序阶段会被调用,通过该方法来比较两个元素之间的顺序
     * @param o 和当前元素比较的相邻元素
     * @return 返回的结果为整型,负值表示当前元素比较大,要排在前面
     * */
    public int compareTo(SortKey o) {
        return o.second - this.second;
    }

    /**
     * 字段的序列化方法
     * String类型的字段需要使用writeUTF
     * */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.first);
        dataOutput.writeInt(this.second);
    }

    /**
     * 字段的反序列化方法
     * */
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return this.first.hashCode() + this.second.hashCode();
    }

    @Override
    public String toString() {
        return this.first + "\t" + this.second;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SortKey)) {
            return false;
        }
        SortKey sortKey = (SortKey) obj;
        return ((this.first.equals(sortKey.first)) && this.second.equals(sortKey.second));
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }
}
