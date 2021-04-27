### Hbase发送器  
Hbase发送器可以将来自上游通道的数据记录发送到Hbase单点服务器或集群服务器上。发送到Hbase数据库中的数据，其底层将启用HDFS分布式文件系统进行存储。HDFS存储系统的天然灾备能力为HBase数据库系统提供了强大的高可靠性。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/HbaseSink/dst/hbaseSink.zip -d /install/zip/  
unzip  /install/zip/hbaseSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/hbaseSink/sink.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|parse|是否解析|true|默认值为true表示解析记录为字典，否则按json字串反编译为字典|
|hostList|连接地址列表|127.0.0.1:2181|用于连接Hbase服务端的地址(单点)或地址列表(集群)|
|fieldMap|字段定义字典|无|当parse=true时，用于定义解析字段名的字典映射表|
|batchSize|批量尺寸|无|批量插入数据库的数据记录数量，若为空则使用单条记录发送方式|
|numFields|数值转换字段|无|插入数据库之前需要转换到数值类型的字段|
|timeFields|时间转换字段|无|插入数据库之前需要转换到时间戳类型的字段|
|defaultTab|默认库表|default:defaultTab|当操作的库表未配置时默认访问的库表名称|
|autoCreate|自动创建|true|当操作的库表不存在时是否允许自动创建库表结构|
|parseFields|解析字段表|无|当parse=true时需要定义的解析字段名列表|
|maxVersion|版本数量|3|键值的最大版本，亦指NOSQL键值的版本数量|
|fieldSeparator|字段分隔符|#|parse=true时，用于解析上游数据记录的字段分隔符|
|maxRetryTimes|重试次数|3|插入数据记录到数据库失败后的最大重试次数|
|failMaxTimeMills|失败等待|2000|前后两次重试之间等待的最大时间间隔(单位:毫秒)|
|batchMaxTimeMills|批量等待|15000|批处理过程中等待上游数据的最大时间间隔(单位:毫秒)|

##### 备注：  
1. timeFields、numFields、parseFields和hostList参数值都可以有多项，项与项之间使用英文逗号分隔即可。  
2. hostList参数的地址实际上是Zookeeper集群的地址，该集群用于实现Hbase集群中HMaster例程的高可用。  
3. 往Hbase数据库中插入数据时，Hbase自动将插入的数据按行键rowKey进行排序并插入合适的行序位置上。  
4. Hbase属于NOSQL数据库，其数据库在Hbase中又被称为表空间，表中的键值对数据被存放在行键索引的每个列族表中。  