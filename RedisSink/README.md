### Redis发送器  
Redis发送器可以将来自上游通道的数据记录发送到Redis单点缓存服务器或集群缓存服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。本插件支持将上游通道数据发送到Redis字典或列表的数据结构中去，亦支持通过免密认证或用户名/密码认证方式连接到Redis服务端。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/RedisSink/dst/redisSink.zip -d /install/zip/  
unzip  /install/zip/redisSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/redisSink/sink.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|parse|是否解析|true|默认值为true表示解析数据记录为字典，否则按json字串反编译为字典|
|dbIndex|数据库索引|0|当targetArch=single时，指示在Redis例程中操作的数据库|
|hostList|连接地址列表|127.0.0.1:6379|用于本插件连接Redis服务端的地址(单点)或地址列表(集群)|
|passWord|Redis密码|无|当Redis例程服务需要登录认证时，指定登录Redis例程的密码|
|targetType|存储结构|dict|Redis例程的数据存储类型，可选值：dict(字典)、pipe(管道)|
|targetArch|Redis架构|cluster|Redis例程的部署架构，可选值：single(单点)、cluster(集群)|
|targetField|存储字段|targetObject|当parse=false时，指示通道记录中表针目标存储对象名的字段名|
|targetIndex|存储索引|0|当parse=true时，指示通道记录中表针目标存储对象名的索引|
|maxRedirect|最大重定向|集群节点数-1|当targetArch=cluster时，指示为键寻找目标节点的最大重定向次数|
|defaultTarget|默认存储|all|默认的存储对象名，当targetType=dict时，all表示Redis全局缓存|
|fieldSeparator|字段分隔符|#|当parse=true时，本插件用于解析上游通道数据记录的字段分隔符|
|maxRetryTimes|重试次数|3|本插件插入数据记录到Redis缓存例程服务失败后的最大重试次数|
|failMaxTimeMills|失败等待|2000|插入失败后，本插件前后两次重试之间等待的最大时间间隔(单位:毫秒)|
##### 备注：  
hostList参数值可以有多项，项与项之间使用英文逗号分隔即可；鉴于目前系统框架集成的是Netty5，Lettuce需要Netty4.X的支持，而Netty5又不能向前兼容到Netty4版本，故RedisTemplate底层通信组件选用的是JedisCluster，相较于Lettuce而言，JedisCluster不支持异步事件和哨兵集群，故使用本插件时不要将其对接到哨兵集群的例程上。另外，本插件将所有对象统一序列化为字符串类型后推送到Redis例程服务，从Redis例程服务取出的数据中，字串类型值都带有额外的双引号，同时键及字串类型值都有反斜杠转义，如果我们需要将其反序列化为对象类型，应该先去其额外引号和反斜杠，操作如下：  
```JAVA
//来自Redis例程服务返回的值
String valueFromRedis="{\"uid\":100,\"uname\":\"ligang\",\"weight\":126.55,\"uage\":38}";
ObjectMapper mapper=new ObjectMapper();
//连续两次调用readValue即可将其对象反序列化为字典对象类型
Object transferValue=mapper.readValue(mapper.readValue(valueFromRedis, String.class), Object.class);
//输出java.util.LinkedHashMap
System.out.println(transferValue.getClass().getName());
```
本插件关于上游通道的输入格式举例如下：  
```Text
1、推送到管道
1.1、parse=true输入格式举例:
#一般输入
testlist#{"100":126}
testlist#{"100":126}#{"weight":122,"score":666}
#矩阵行输入
testlist#{"uid":100,"uname":"ligang","uage":38,"weight":126.55}#{"uid":101,"uname":"zhanghua","uage":28,"weight":122.55}

1.2、parse=false输入格式举例:
#一般输入
{"targetObject":"testlist","weight":122,"score":666}
#数据库输入
{"targetObject":"testlist","uid":100,"uname":"ligang","uage":38,"weight":126.55}

2、推送到字典
2.1、parse=true输入格式举例:
#一般输入
testdict#{"100":126}
testdict#{"weight":122,"score":666}
#数据库输入
testdict#{"100":{"uid":100,"uname":"ligang","uage":38,"weight":126.55},"101":{"uid":101,"uname":"zhanghua","uage":28,"weight":122.55}}

2.2、parse=false输入格式举例:
#一般输入
{"targetObject":"testdict","weight":122,"score":666}
#矩阵行输入
{"targetObject":"testdict","100":{"uid":100,"uname":"ligang","uage":38,"weight":126.55},"101":{"uid":101,"uname":"zhanghua","uage":28,"weight":122.55}}
```