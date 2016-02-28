# MapReduce Demo

Map-Reduce程序场景代码。

已完成的有：

> 1.网站kpi数据统计   
> 2.电信运营商用户基站停留数据统计   
> 3.基于物品的协同过滤实现  
> 4.测试mahout推荐算法API

##1.网站kpi数据统计

程序中分别对五个kpi指标进行统计操作：   
> 1.browser：用户使用的浏览器统计   
> 2.ips：页面用户独立ip数统计   
> 3.pv：网站pv量统计   
> 4.source：用户来源网址统计   
> 5.time：时间段用户访问量统计   

[数据下载][2]

##2.电信运营商用户基站停留数据统计

原始数据分为位置和网络两种   
位置数据格式为：    
用户标识 设备标识 开关机信息 基站位置 通讯的时间   
example:   
0000009999  0054785806  3   00000089    2016-02-21 21:55:37

网络数据格式为：   
用户标识 设备标识 基站位置 通讯的时间 访问的URL   
example:   
0000000999  0054776806  00000109    2016-02-21 23:35:18 www.baidu.com

需要得到的数据格式为：   
用户标识 时段 基站位置 停留时间

example:   
00001 09-18 00003 15   
用户00001在09-18点这个时间段在基站00003停留了15分钟

两个reducer：

> 1.统计每个用户在不同时段中各个基站的停留时间   
> 2.在1的结果上只保留停留时间最长的基站位置信息   

[数据下载][3]

##3.基于物品的协同过滤实现

算法解释请看：[ItermCF的MR并行实现][4]

##4.测试mahout推荐算法API

> 1.RecommendFactory:推荐相关信息获取的工厂类,包括用户/物品相似度，用户邻居，用户/物品推荐器，数据模型，算法评分器
> 2.RecommendUtil:打印算法的评分结果、推荐结果等
> 3.RecommendEvaluator:从RecommendFactory中获得相似度、推荐器等进行算法组合，借助RecommendUtil打印出评分结果
> 4.RecommendResult:选取RecommendEvaluator中评分结果最好的两个算法，借助RecommendUtil打印出推荐结果

[数据下载][5]

详情见代码

作者：[@小黑][1]

[1]:http://www.xiaohei.info
[2]:http://download.csdn.net/detail/qq1010885678/9439530
[3]:http://download.csdn.net/detail/qq1010885678/9439587
[4]:http://blog.csdn.net/qq1010885678/article/details/50751607
[5]:http://download.csdn.net/detail/qq1010885678/9446510
