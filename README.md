# MapReduce Demo

Map-Reduce程序场景代码。

已完成的有：

> 1.网站kpi数据统计   
> 2.电信运营商用户基站停留数据统计   
> 3.基于物品的协同过滤实现  
> 4.测试mahout推荐算法API    
> 5.使用自定义的分片策略和庖丁分词进行中文分析  
> 6.PeopleRank算法并行化实现-mr的矩阵计算   
> 7.简单实现sql的统计、groupby和join   
> 8.实现简单的倒排索引   
> 9.查找社交二度关系   
> 10.广告精准营销

## 1.网站kpi数据统计

程序中分别对五个kpi指标进行统计操作：   
> 1.browser：用户使用的浏览器统计   
> 2.ips：页面用户独立ip数统计   
> 3.pv：网站pv量统计   
> 4.source：用户来源网址统计   
> 5.time：时间段用户访问量统计   

[数据下载][2]

## 2.电信运营商用户基站停留数据统计

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

## 3.基于物品的协同过滤实现

算法解释请看：[ItermCF的MR并行实现][4]

## 4.测试mahout推荐算法API

> 1.RecommendFactory:推荐相关信息获取的工厂类,包括用户/物品相似度，用户邻居，用户/物品推荐器，数据模型，算法评分器   
> 2.RecommendUtil:打印算法的评分结果、推荐结果等   
> 3.RecommendEvaluator:从RecommendFactory中获得相似度、推荐器等进行算法组合，借助RecommendUtil打印出评分结果   
> 4.RecommendResult:选取RecommendEvaluator中评分结果最好的两个算法，借助RecommendUtil打印出推荐结果   

###**调用mahout推荐算法流程:**

> 1.选择输入数据，创建DataModel对象   
> 2.根据DataModel对象创建用户或物品的Similarity对象   
> 3.如果是基于用户的，那么需要创建UserNeighborhood对象   
> 4.根据以上的条件，创建用户或物品的Recommender对象    

**使用算法评估器流程：**    

> 1.123步骤同上   
> 4.创建RecommenderBuilder对象，调用RecommenderEvaluator的evaluate方法进行评估   
> 4.1评估查全率和召回率创建RecommenderIRStatsEvaluator对象，调用其evaluate方法返回IRStatistics对象   
> 4.2调用IRStatistics对象的getPrecision,getRecall方法即可   

**使用条件过滤的流程：**

> 1.使用自定义的类实现IDRescorer接口，在类中定义成员，在类外使用过滤规则，将过滤之后的结果存入类中     
> 2.调用recommend方法进行推荐的时候加入参数IDRescorer    

[数据下载][5]

## 5.使用自定义的分片策略和庖丁分词进行中文分析

[庖丁分词需要的jar包和dic文件下载][7]

**庖丁分词的使用注意:**    
> 1.paoding-analysis的包和mahout的包放在一起会冲突，mahout中重写了tokenStream方法为final类型，paoding中又需要重写此方法，会出现class net.paoding.analysis.analyzer.PaodingAnalyzerBean overrides final method tokenStream.的错误   
    **解决方法:**去掉mahout的包即可    

> 2.在IDEA项目中，需要把庖丁的dic目录放入src/main/resources下，否则会报PaodingAnalysisException: not found the dic home directory!的异常，修改paoding-analysis包中的paoding-dic-home文件，设置paoding.dic.home=classpath:dic    

> 3.打成jar包时，需要将dic目录复制到hadoop的classpath目录下（最好是第一个），否则会报PaodingAnalysisException: dic home should not be a file, but a directory!，虽然jar包中已经包含了dic。

**包说明：**    
> 1.normal：使用默认的mapreduce分切策略，读取全部小文件需要8k+个mapper，cpu占用高达80%，运行时间n个小时    
> 2.special：使用自定义的分片策略，将多个小文件合并，每次map处理的数据为一个文件的全部内容，而不是一行，启动的mapper为4个，运行时间大大提高    

[自定义的分片策略解决大量小文件的问题][8]

[数据下载][6]

## 6.PeopleRank算法并行化实现-mr的矩阵计算

使用mapreduce框架实现PeopleRank算法，并在示例数据集上进行测试。

PeopleRank算法实现过程：   
> 1.AdjacencyMapper：将原始数据集转换成邻接表   
> 2.AdjacencyReducer：计算邻接表的每一行，转换成邻接矩阵之后使用PageRank的计算公式推导邻接概率矩阵   
> 3.CalcPeopleRankMapper：输入邻接概率矩阵和pr矩阵，运用矩阵相乘规律将数据输出到reduce进行计算  
> 4.CalcPeopleRankReducer：分别用两个map保存邻接矩阵和pr矩阵的值进行计算   
> 5.FinallyResultMapper：将计算得到的pr值统一输出到reduce中进行转换计算   
> 6.FinallyResultReducer：对每个pr值/pr总值，得到的数据即为最终结果

该算法重点在于**矩阵的计算**    
整个mapreduce作业中最核心的就是**CalcPeopleRank**这部分   
在mapreduce中进行矩阵计算的技巧在于**从矩阵乘法公式中找出矩阵相乘的规律**   

map过程：   
> 1.读取第一个矩阵的每一行的**每一个数据**，并做标识处理    
> 2.**将行号作为key**，读取第二个矩阵的时候按照**列**读，**以列号为key**，这样一来两个矩阵中**对应的需要计算的值**都都被一起输出到reduce中   
> 3.由于两个矩阵的值都会出现在reduce中，所以需要在map的value中设置一个**标识位**，如：A,B等，表示这个数值是第一个矩阵还是第二个矩阵的   
> 4.即使将两个矩阵的行和列对应起来了，但是没有**将行列中各个值对应起来**也是没办法计算的，所以map得value中还应该**包含该数值在当前矩阵中是第几列**（对于第二个按列读取的矩阵来说就是第几行）

reduce过程：   
> 1.每个输入都是**两个矩阵相对应的行和列**，并且从value中可以得到该值是哪个矩阵，第几行第几列的值   
> 2.根据value中**矩阵的标志位**对不同的矩阵值做不同的处理，分别加入**两个map字典**中，key为原本value中包含的行列标志位，value为数值本身   
> 3.现在只要遍历任意一个map，**取出一个值的时候就根据该值的key到另外一个map中取出对应的值进行相乘**，最后将结果相加即可

[数据下载][9]

## 7.简单实现sql的统计、groupby和join

### 统计最大、最小和平均值

根据给出的文件（表），内容格式为   

|Name|age|
|---|---|
|abc|12|
|...|...|

统计年龄的最大、最小和平均值   
sql示例:   
```sql
select avg(age) as avg,max(age) as max,min(age) as min from xxx;
```

### group by

根据给出的文件（表），内容格式为   

|customer|order_price|
|---|---|
|1|100|
|2|130|
|...|...|

实现根据costomer进行分组，统计每个customer的总订单金额   
sql示例:   
```sql
select customer,sum(order_price) from orders group by customer
```

### join连接

根据给出的文件（表）   
Customer表结构为:   

|id|name|
|---|---|
|1|chubby|
|2|xiaohei|
|...|...|

Orders表结构为:   

|id|cus_id|
|1|1|
|2|1|
|3|2|
|...|...|

实现两个表的join连接   
sql示例:

```sql
select Customer.name,Orders.id from Customers left join Orders on Customers.id=Orders.cus_id
```

## 8.实现简单的倒排索引

倒排索引的详情参考：   
[mapreduce实现搜索引擎简单的倒排索引](http://www.xiaohei.info/2015/03/19/mapreduce-inverted-index/)

## 9.查找社交二度关系

一个简单的类似QQ好友推荐的功能，思想就是找到好友之间的二度关系，例如有如下好友关系：   
小明 小红   
张三 李四   
王五 小李   
小红 小黑
小李 小红

以小明为例，关系图为：小明->小红,小红->小黑   
那么小明和小黑之间就有一个二度关系，可以互相推荐好友（P.S. 著名的xx说世界上的任何两个人最多通过7层关系都可以联系起来）

我们要做的事情就是将有二度关系的人找出来   
首先要确定map的key和value应该是什么才能在reduce中得到需要的结果   
根据mr程序的尿性，在reduce的时候会将key相同的value聚集在一起，那么可以将二度关系中的链接点作为key，reduce中就可以拿到推荐的好友   
如上例中的小红为key，小明和小黑为value

mr程序很简单：

map过程中将每行的1和2作为key-value输出一次，反过来将2和1作为key-value再次输出   
reduce中可以拿到所有key相同的一个集合，集合中的每个元素都是有二度关系的，对其进行全排列即可得到推荐好友名单

## 10.广告精准营销

案例说明请参考：[MapReduce广告精准营销案例](http://www.xiaohei.info/2016/04/08/mapreduce-weibo-ad/)

详情见代码

作者：[@小黑][1]

[1]:http://www.xiaohei.info
[2]:http://download.csdn.net/detail/qq1010885678/9439530
[3]:http://download.csdn.net/detail/qq1010885678/9439587
[4]:http://blog.csdn.net/qq1010885678/article/details/50751607
[5]:http://download.csdn.net/detail/qq1010885678/9446510
[6]:http://download.csdn.net/detail/qq1010885678/9447741
[7]:http://download.csdn.net/detail/qq1010885678/9448143
[8]:http://blog.csdn.net/qq1010885678/article/details/50771361
[9]:http://download.csdn.net/detail/qq1010885678/9456762
