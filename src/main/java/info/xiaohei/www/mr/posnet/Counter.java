package info.xiaohei.www.mr.posnet;

/**
 * Created by xiaohei on 16/2/22.
 *
 * 异常计数的类型
 */
public enum Counter {
    TIMESKIP,		//时间格式有误
    OUTOFTIMEFLASGSKIP,	//时间超出最大时段
    LINESKIP,		//源文件行有误
    OUTOFTIMESKIP,   //不在当前时间内
    TIMEFORMATERR   //时间格式化错误
}
