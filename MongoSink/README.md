### Mongo发送器  
Mongo发送器可以将来自上游通道的数据记录发送到MongoDB单点服务器或集群服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。本插件支持通过免密认证和用户名/密码认证方式连接到MongoDB服务端。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/MongoSink/dst/mongoSink.zip -d /install/zip/  
unzip  /install/zip/mongoSink.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/mongoSink/sink.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|parse|是否解析|true|默认值为true表示解析数据记录为字典，否则按json字串反编译为字典|
|idField|文档ID字段|无|写入文档库表记录中的主键字段名称，若为NULL则由数据库自动生成|
|dbField|数据库字段名|无|数据记录中的数据库字段名，若为NULL则取defaultDB参数值|
|tabField|数据表字段名|无|数据记录中的数据库字段名，若为NULL则取defaultTab参数值|
|hostList|连接地址列表|127.0.0.1:27017|用于连接MongoDB服务端的地址(单点)或地址列表(集群)|
|fieldList|字段名列表|无|parse=true时，用于解析上游数据记录对应的字段名列表|
|batchSize|批量尺寸|无|批量插入数据库的数据记录数量，若为空则使用单条记录发送方式|
|timeZone|时区偏差|+0800|数据库时间字段相对于格林威治时间偏差，默认为东八区|
|userName|登录用户|无|用户名或密码只要有一个为空则使用免密登录模式|
|passWord|登录密码|无|用户名或密码只要有一个为空则使用免密登录模式|
|numFields|数值转换字段|无|插入数据库之前需要转换到数值类型的字段|
|timeFields|时间转换字段|无|插入数据库之前需要转换到时间戳类型的字段|
|defaultDB|默认数据库|test|访问的默认数据库为test，登录的默认数据库为admin|
|defaultTab|默认数据表|test|访问的默认数据表为test|
|fieldSeparator|字段分隔符|所有空白字符|parse=true时，用于解析上游数据记录的字段分隔符|
|connectTimeout|下发超时|30000|连接到MongoDB服务端的最大等待超时时间(单位:毫秒)|
|maxRetryTimes|重试次数|3|插入数据记录到数据库失败后的最大重试次数|
|failMaxTimeMills|失败等待|2000|前后两次重试之间等待的最大时间间隔(单位:毫秒)|
|batchMaxTimeMills|批量等待|2000|批处理过程中等待上游数据的最大时间间隔(单位:毫秒)|
##### 备注：  
timeFields、numFields、fieldList和hostList参数值都可以有多项，项与项之间使用英文逗号分隔即可；另外本插件还支持很多相关MongoDB的连接参数项，这些参数项的名称与MongoDB官方参数名相同，有兴趣者可以自行测试之。  