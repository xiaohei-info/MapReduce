package info.xiaohei.www.mr;

import info.xiaohei.www.mr.posnet.Counter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Created by xiaohei on 16/2/23.
 * 通用工具类
 */
public class HadoopUtil {

    /**
     * 分隔符类型,使用正则表达式,表示分隔符为\t或者,
     * 使用方法为SPARATOR.split(字符串)
     */
    public static final Pattern SPARATOR = Pattern.compile("[\t,]");

    /**
     * HDFS路径的根目录
     */
    public static final String HDFS = "hdfs://localhost:9000";

    /**
     * 计算unixtime两两之间的时间差
     *
     * @param sortDatas key为unixtime,value为pos
     * @return key为pos, value为该pos的停留时间
     */
    public static HashMap<String, Float> calcStayTime(TreeMap<Long, String> sortDatas) {
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

    /**
     * 将map阶段传递过来的数据按照unixtime从小到大排序(使用TreeMap)
     *
     * @param context reducer的context上下文,用于设置counter
     * @param values  map阶段传递过来的数据
     * @return key为unixtime, value为pos
     */
    public static TreeMap<Long, String> getSortedData(Reducer.Context context, Iterable<Text> values) {
        TreeMap<Long, String> sortedData = new TreeMap<Long, String>();
        for (Text v : values) {
            String[] vs = v.toString().split(",");
            try {
                sortedData.put(Long.parseLong(vs[1]), vs[0]);
            } catch (NumberFormatException num) {
                context.getCounter(Counter.TIMESKIP).increment(1);
            }
        }
        return sortedData;
    }

    /**
     * 在分布式缓存中得到指定缓存的文件,如果建立了符号连接,则直接根据symlink得到,如果不是,则使用URI的方式获得
     *
     * @param context                  获得缓存文件的对象
     * @param symLink                  简单文件名,如foo.txt
     * @param throwExceptionIfNotFound 是否抛出异常
     * @return 返回得到的文件
     * @throws IOException
     */
    public static File findDistributedFileBySymlink(JobContext context, String symLink, boolean throwExceptionIfNotFound) throws IOException {
        URI[] uris = context.getCacheFiles();
        if (uris == null || uris.length == 0) {
            if (throwExceptionIfNotFound)
                throw new RuntimeException("Unable to find file with symlink '" + symLink + "' in distributed cache");
            return null;
        }
        URI symlinkUri = null;
        for (URI uri : uris) {
            if (symLink.equals(uri.getFragment())) {
                symlinkUri = uri;
                break;
            }
        }
        if (symlinkUri == null) {
            if (throwExceptionIfNotFound)
                throw new RuntimeException("Unable to find file with symlink '" + symLink + "' in distributed cache");
            return null;
        }
        //如果是getScheme返回的标识为file的话,那么要使用文件系统的完整路径来获得,如果不是,则就是建立了符号连接的文件
        return "file".equalsIgnoreCase(FileSystem.get(context.getConfiguration()).getScheme()) ? (new File(symlinkUri.getPath())) : new File(symLink);

    }
}
