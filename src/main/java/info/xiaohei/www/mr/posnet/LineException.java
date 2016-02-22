package info.xiaohei.www.mr.posnet;

/**
 * Created by xiaohei on 16/2/21.
 *
 * 自定义的异常类,提供异常统计帮助
 */
public class LineException extends Exception {

    //异常的标识
    //-1:不是当前日期内
    //0:时间格式不正确
    //1:时间坐在的小时超出最大的时段
    private Integer flag;

    public LineException(String msg, Integer flag) {
        super(msg);
        this.flag = flag;
    }

    public Integer getFlag() {
        return flag;
    }
}
