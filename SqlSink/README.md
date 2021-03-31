### SQL发送器  
SQL发送器可以将来自上游通道的数据记录发送到基于标准SQL支持的关系数据库服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。本插件支持通过免密认证和用户名/密码认证方式连接到关系数据库服务端。  
      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/SqlSink/dst/sqlSink.zip -d /install/zip/  
unzip  /install/zip/sqlSink.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/sqlSink/sink.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|parse|是否解析|true|默认值为true表示解析数据记录为字典，否则按json字串反序列化为字典|
|dbField|数据库字段|无|parse=false时，用于从记录字典提取数据库名称的字段名称|
|tabField|数据表字段|无|parse=false时，用于从记录字典提取数据表名称的字段名称|
|dbIndex|数据库索引|无|parse=true时，用于解析数据库名称的记录字段索引|
|tabIndex|数据表索引|无|parse=true时，用于解析数据表名称的记录字段索引|
|batchSize|批量尺寸|无|每个批次推送的文档数量，若为NULL则每条记录推送一次|
|userName|登录用户|无|登录数据库服务用户名，用户名或密码为NULL则免密登录数据库|
|passWord|登录密码|无|登录数据库服务密码，用户名或密码为NULL则免密登录数据库|
|defaultDB|默认数据库|无|当数据记录中无法解析或提取数据库名称时使用该默认值|
|defaultTab|默认数据表|无|当数据记录中无法解析或提取数据表名称时使用该默认值|
|jdbcDriver|JDBC驱动|MySql驱动|本插件客户端用于访问SQL关系数据库的驱动程序|
|fieldSeparator|字段分隔符|所有空白字符|parse=true时，用于解析上游通道记录的字段分隔符|
|maxRetryTimes|重试次数|3|推送数据记录到SQL关系数据库失败后的最大重试次数|
|failMaxTimeMills|失败等待|2000|推送数据记录失败后，前后两次重试之间的时间间隔(单位:毫秒)|
|connectionString|连接地址|MySql连接|本插件用于访问SQL关系数据库的连接地址字符串，地址可附带连接参数|
|batchMaxTimeMills|批次等待|2000|本插件支持批量推送，该参数为等待每个批次的最大时间阈值(单位:毫秒)|
##### 备注：  
1. 若关系数据库为主从同步架构，请将本插件对接到主库服务器上。  
2. 若关系数据库启用了代理服务，请将本插件对接到代理服务器上。  
3. jdbcDriver的默认值为常用关系数据库MySql的驱动，即：com.mysql.cj.jdbc.Driver  
4. connectionString的默认值为常用关系数据库MySql的本地连接字符串，即：  
jdbc:mysql://127.0.0.1:3306/?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&useSSL=false&  
serverTimezone=GMT%2B8  