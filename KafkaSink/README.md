### KafKa发送器  
KafKa发送器可以将来自上游通道的数据记录发送到KafKa消息中间件服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。由于KafKa中间件自身不支持认证，故本插件均通过免密认证方式连接到KafKa的Broker集群节点。  
      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/KafkaSink/dst/kafkaSink.zip -d /install/zip/  
unzip  /install/zip/kafkaSink.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/kafkaSink/sink.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|parse|是否解析|true|true表示解析记录为字典，否则按json字串反序列化为字典|
|hostList|地址列表|127.0.0.1:9092|连接KafKa服务端的单点地址(单点)或地址列表(集群)|
|ackLevel|ACK级别|1|ACK级别可选值:0(无需确认)、1(保存后确认)、-1(所有副本接收后确认)|
|topicField|主题字段|topic|parse=false时，用于从记录字典提取主题名称的字段名称|
|topicIndex|主题索引|0|parse=true时，用于解析主题名称的记录字段索引|
|defaultTopic|默认主题|无|当数据记录中无法解析或提取主题名称时使用该默认值|
|fieldSeparator|字段分隔符|,|parse=true时，用于解析上游通道记录的字段分隔符|
|maxRetryTimes|重试次数|3|推送数据记录到KafKa中间件失败后的最大重试次数|
|autoCreateTopic|自创主题|true|当KafKa服务器上没有访问的主题时是否自动创建主题|
|partitionNumPerTopic|分区数量|1|每个主题的默认分区数量，分区用于表针并行度的高低|
##### 备注：  
fieldSeparator参数的默认值是英文逗号，一般针对消息中间件的Sink插件只需要区分主题字段即可，并且以记录中首次出现的分隔符为准来分隔主题字段，所以默认使用英文逗号即可；而对于针对存储或数据库的Sink插件由于需要区分过多的机构化字段，故默认使用空白字符。  