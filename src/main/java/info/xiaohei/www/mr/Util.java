package info.xiaohei.www.mr;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Created by xiaohei on 16/2/23.
 * 通用工具类
 */
public class Util {

    /**
     * 计算unixtime两两之间的时间差
     *
     * @param sortDatas key为unixtime,value为pos
     * @return key为pos, value为该pos的停留时间
     */
    protected HashMap<String, Float> calcStayTime(TreeMap<Long, String> sortDatas) {
        HashMap<String, Float> resMap = new HashMap<String, Float>();
        Iterator<Long> iter = sortDatas.keySet().iterator();
        Long currentTimeflag = iter.next();
        //遍历treemap
        while (iter.hasNext()) {
            Long nextTimeflag = iter.next();
            float diff = (nextTimeflag - currentTimeflag) / 60.0f;
            //超过60分钟过滤不计
            if (diff <= 60.0) {
                String currentPos = sortDatas.get(currentTimeflag);
                if (resMap.containsKey(currentPos)) {
                    resMap.put(currentPos, resMap.get(currentPos) + diff);
                } else {
                    resMap.put(currentPos, diff);
                }
            }
            currentTimeflag = nextTimeflag;
        }
        return resMap;
    }
}
