package info.xiaohei.www;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

/**
 * Created by xiaohei on 16/2/24.
 * 用于BaseDriver初始化job的需求(多个job的情况下)
 */
public class JobInitModel {
    private String[] inPaths;//程序的输入目录
    private String outPath;//程序的输出目录
    private Configuration conf;//配置信息
    private Job job;//job相关设置,如分布式文件缓存共享等
    private String jobName;//该job的名称
    private Class<?> jarClass;//mr的驱动程序类
    private Class<? extends InputFormat> inputFormatClass;//输入格式化类
    private Class<? extends Mapper> mapper;//mapper的实现类
    private Class<?> mapOutKeyClass;//mapper输出的key类型
    private Class<?> mapOutValueClass;//mapper输出的value类型
    private Class<? extends Reducer> combinerClass;
    private Class<? extends Partitioner> partitionerClass;
    private Class<? extends Reducer> reducer;//reducer的实现类
    private Class<?> reduceOutKeyClass;//reduce输出的key类型
    private Class<?> reduceOutValueClass;//reduce输出的value类型

    public JobInitModel() {
    }

    public JobInitModel(String[] inPaths, String outPath, Configuration conf, Job job, String jobName
            , Class<?> jarClass, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapper
            , Class<?> mapOutKeyClass, Class<?> mapOutValueClass, Class<? extends Partitioner> partitionerClass
            , Class<? extends Reducer> combiner, Class<? extends Reducer> reducer, Class<?> reduceOutKeyClass
            , Class<?> reduceOutValueClass) {
        this.outPath = outPath;
        this.inPaths = inPaths;
        this.conf = conf;
        this.job = job;
        this.jobName = jobName;
        this.jarClass = jarClass;
        this.inputFormatClass = inputFormatClass;
        this.mapper = mapper;
        this.mapOutKeyClass = mapOutKeyClass;
        this.mapOutValueClass = mapOutValueClass;
        this.partitionerClass = partitionerClass;
        this.combinerClass = combiner;
        this.reducer = reducer;
        this.reduceOutKeyClass = reduceOutKeyClass;
        this.reduceOutValueClass = reduceOutValueClass;
    }

    public String[] getInPaths() {
        return inPaths;
    }

    public void setInPaths(String[] inPaths) {
        this.inPaths = inPaths;
    }

    public String getOutPath() {
        return outPath;
    }

    public void setOutPath(String outPath) {
        this.outPath = outPath;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Class<?> getJarClass() {
        return jarClass;
    }

    public void setJarClass(Class<?> jarClass) {
        this.jarClass = jarClass;
    }

    public Class<? extends Mapper> getMapper() {
        return mapper;
    }

    public void setMapper(Class<? extends Mapper> mapper) {
        this.mapper = mapper;
    }

    public Class<?> getMapOutKeyClass() {
        return mapOutKeyClass;
    }

    public void setMapOutKeyClass(Class<?> mapOutKeyClass) {
        this.mapOutKeyClass = mapOutKeyClass;
    }

    public Class<?> getMapOutValueClass() {
        return mapOutValueClass;
    }

    public void setMapOutValueClass(Class<?> mapOutValueClass) {
        this.mapOutValueClass = mapOutValueClass;
    }

    public Class<? extends Reducer> getReducer() {
        return reducer;
    }

    public void setReducer(Class<? extends Reducer> reducer) {
        this.reducer = reducer;
    }

    public Class<?> getReduceOutKeyClass() {
        return reduceOutKeyClass;
    }

    public void setReduceOutKeyClass(Class<?> reduceOutKeyClass) {
        this.reduceOutKeyClass = reduceOutKeyClass;
    }

    public Class<?> getReduceOutValueClass() {
        return reduceOutValueClass;
    }

    public void setReduceOutValueClass(Class<?> reduceOutValueClass) {
        this.reduceOutValueClass = reduceOutValueClass;
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
    }

    public Class<? extends Reducer> getCombinerClass() {
        return combinerClass;
    }

    public void setCombinerClass(Class<? extends Reducer> combinerClass) {
        this.combinerClass = combinerClass;
    }

    public Class<? extends Partitioner> getPartitionerClass() {
        return partitionerClass;
    }

    public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) {
        this.partitionerClass = partitionerClass;
    }
}
