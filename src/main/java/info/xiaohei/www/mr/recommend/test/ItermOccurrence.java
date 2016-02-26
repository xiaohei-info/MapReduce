package info.xiaohei.www.mr.recommend.test;

/**
 * Created by xiaohei on 16/2/25.
 *
 */
public class ItermOccurrence {
    private String itemId1;
    private String itemId2;
    private int occurrence;

    public ItermOccurrence() {
    }

    public ItermOccurrence(String itemId1, String itemId2, int occurrence) {
        this.itemId1 = itemId1;
        this.itemId2 = itemId2;
        this.occurrence = occurrence;
    }

    public String getItemId1() {
        return itemId1;
    }

    public void setItemId1(String itemId1) {
        this.itemId1 = itemId1;
    }

    public String getItemId2() {
        return itemId2;
    }

    public void setItemId2(String itemId2) {
        this.itemId2 = itemId2;
    }

    public int getOccurrence() {
        return occurrence;
    }

    public void setOccurrence(int occurrence) {
        this.occurrence = occurrence;
    }
}
