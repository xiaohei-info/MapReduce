package info.xiaohei.www.mr.posnet;

import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by xiaohei on 16/2/21.
 *
 * 根据每行数据格式化,生成对应的实体对象
 * 主要提供format方法进行数据格式化
 */
public class LineFormater {

    private String imsi;//用户标识
    private String pos;//位置信息
    private String time;//原始时间信息
    private String timeflag;//所需要的时间段信息,从time字段中得到
    private Date day;//所需要的时间信息,需要转为unix格式,从time字段中德奥
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 数据格式化的方法,并提供了验证数据合法性的流程
     * @param line 每行数据
     * @param isPos 该行数据是POS还是NET
     * @param date 要处理的日志(只处理这个日期的数据)
     * @param timepoint 要统计的时间段,格式为[09,18,24],表示统计00-09,09-18,18-24三个时间段的信息
     * */
    public void format(String line, Boolean isPos, String date, String[] timepoint) throws LineException {
        String[] words = line.split("\t");
        //根据不同数据的格式截取需要的信息
        if (isPos) {
            this.imsi = words[0];
            this.pos = words[3];
            this.time = words[4];
        } else {
            this.imsi = words[0];
            this.pos = words[2];
            this.time = words[3];
        }
        //如果不是当前日期的数据,则过滤并统计异常
        if (!this.time.startsWith(date)) {
            throw new LineException("Incorrect datetime!", -1);
        }
        //将字符串的time字段转成时间类型的day字段,如果该时间格式不正确,则过滤并统计异常
        try {
            this.day = this.simpleDateFormat.parse(this.time);
        } catch (ParseException e) {
            throw new LineException("Incorrect date format!", 0);
        }

        //根据time字段计算timeflag过程
        //从time中获得当前的小时信息
        Integer hour = Integer.valueOf(time.split(" ")[1].split(":")[0]);
        //在数据长度中循环
        for (int i = 0; i < timepoint.length; i++) {
            if (Integer.parseInt(timepoint[i]) <= hour) {
                try {
                    //如果大于当前时间段,则暂时设置为[当前时间]-[当前时段+1],在下个循环中继续判断
                    //需要考虑到hour大于最大时段的情况,抛出异常进行统计
                    this.timeflag = timepoint[i] + "-" + timepoint[i + 1];
                } catch (Exception ex) {
                    throw new LineException("Current hour is bigger than the max-timepoint!", 1);
                }
            } else {
                //如果小于当前时段,则必定是[当前时段-1]-[当前时段]
                //如果是一次循环,则应该是从00时段开始
                if (i == 0) {
                    this.timeflag = "00-" + timepoint[i];
                } else {
                    this.timeflag = timepoint[i - 1] + "-" + timepoint[i];
                }
                break;
            }
        }
    }

    /**
     * 将imsi和timeflag作为key输出
     * */
    public Text outKey() {
        return new Text(this.imsi + "," + this.timeflag);
    }

    /**
     * 将pos和day()
     * */
    public Text outValue() {
        return new Text(this.pos + "," + this.day.getTime() / 1000L);
    }
}
