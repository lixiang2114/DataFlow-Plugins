### Elastic发送器  
Elastic发送器可以将来自上游通道的数据记录发送到Elastic单点服务器或集群服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。本插件支持通过免密认证和用户名/密码认证方式连接到Elastic服务端。  
      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/ElasticSink/dst/elasticSink.zip -d /install/zip/  
unzip  /install/zip/elasticSink.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/elasticSink/sink.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|parse|是否解析|true|默认值为true表示解析数据记录为字典，否则按json字串反序列化为字典|
|idField|文档ID字段|无|写入文档库表记录中的主键字段名称，若为NULL则由数据库自动生成|
|fieldList|字段列表|无|parse=true时，用于解析上游通道记录的字段映射表|
|hostList|主机地址|无|连接Elastic的主机地址或集群地址，多个地址使用英文逗号分隔|
|typeField|索引类型字段|无|用于指示推送数据的目标索引库类型字段名|
|indexField|索引库字段|无|用于指示推送数据的目标索引库字段名|
|timeFields|时间字段|无|推送之前需要转换为时间戳类型的字段|
|numFields|数值字段|无|推送之前需要转换为数值类型的字段|
|batchSize|批量推送尺寸|无|每个批次推送的文档数量，若为NULL则每条记录推送一次|
|timeZone|时区差值|无|时间字段值与格林威治之间时差，中国为东八区，即:+0800|
|userName|登录用户|无|用于登录Elastic服务器的用户名|
|passWord|登录密码|无|用于登录Elastic服务器的密码|
|defaultType|默认索引类型|\_doc|用于指示推送数据的默认索引库类型表名称|
|defaultIndex|默认索引|test|用于指示推送数据的默认索引库名称|
|clusterName|集群名称|无|当前插件连接的Elastic集群名称|
|fieldSeparator|字段分隔符|所有空白字符|parse=true时，用于解析上游通道记录的字段分隔符|
|maxRetryTimes|重试次数|3|推送数据失败后的最大重试次数|
|failMaxTimeMills|失败等待|2000|前后两次重试之间的时间间隔(单位:毫秒)|
|enableAuthCache|认证缓存|无|是否可以缓存登录认证信息|
|batchMaxTimeMills|批次等待|2000|每一个批次等待的最大时间(单位:毫秒)|
##### 备注：  
timeFields、numFields、fieldList和hostList参数值都可以有多项，项与项之间使用英文逗号分隔即可。  