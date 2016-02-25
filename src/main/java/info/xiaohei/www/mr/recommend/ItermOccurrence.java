package info.xiaohei.www.mr.recommend;

/**
 * Created by xiaohei on 16/2/25.
 * 描述物品同现度的实体类
 */
public class ItermOccurrence {
    private String iterm1;//iterm1的id
    private String iterm2;//iterm2的id
    private Double occurrence;//同现度

    public ItermOccurrence() {
    }

    public ItermOccurrence(String iterm1, String iterm2, Double occurrence) {
        this.iterm1 = iterm1;
        this.iterm2 = iterm2;
        this.occurrence = occurrence;
    }

    public String getIterm1() {
        return iterm1;
    }

    public void setIterm1(String iterm1) {
        this.iterm1 = iterm1;
    }

    public String getIterm2() {
        return iterm2;
    }

    public void setIterm2(String iterm2) {
        this.iterm2 = iterm2;
    }

    public Double getOccurrence() {
        return occurrence;
    }

    public void setOccurrence(Double occurrence) {
        this.occurrence = occurrence;
    }
}
