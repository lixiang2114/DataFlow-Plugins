### SQL收集器  
SQL收集器用于近实时扫描SQL关系数据库中的数据，并将获取的数据记录推送到下游通道。支持免密登录和用户名/密码登录；支持字典JSON字串、查询字串和值序列串三种格式输出。同时亦支持天然的批量扫描和自动近实时扫描模式。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/SqlSource/dst/sqlSource.zip -d /install/zip/  
unzip  /install/zip/sqlSource.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/sqlSource/source.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|realtime|是否实时|true|是否实时扫描读取SQL关系数据库表中的数据，默认支持实时扫描读取|
|phFields|分页字段|无|分页占位符字段，对于关系数据库一般为:startIndex,batchSize[,endIndex]|
|selectSQL|分页SQL|无|本插件用于分页扫描读取数据的SQL语句，不同关系数据库分页语句不同|
|batchSize|批量尺寸|100|每次扫描读取SQL关系数据库表的批次记录数量，该参数同时也为分页尺寸|
|startIndex|起始索引|0|实时扫描读取SQL关系数据库表数据记录的起始索引，该参数亦为分页参数|
|userName|登录用户|无|登录SQL关系数据库的用户名，用户名或密码为NULL启用免密登录|
|passWord|登录密码|无|登录SQL关系数据库的用户密码，用户名或密码为NULL启用免密登录|
|outFormat|输出格式|qstr|格式可选值:qstr(查询字串)、map(json字串)，若为无法识别则使用值序列格式|
|jdbcDriver|JDBC驱动|MySql驱动|本插件客户端用于访问SQL关系数据库的驱动程序，默认为MySql驱动程序|
|connectionString|连接地址|MySql连接|本插件用于访问SQL关系数据库的连接地址字符串，地址可附带连接参数|
##### 备注：  
1. 若关系数据库为主从同步架构，请将本插件对接到从库服务器上。
2. 若关系数据库启用了代理服务，请将本插件对接到代理服务器上。
3. jdbcDriver的默认值为常用关系数据库MySql的驱动，即：com.mysql.cj.jdbc.Driver
4. connectionString的默认值为常用关系数据库MySql的本地连接字符串，即：
jdbc:mysql://127.0.0.1:3306/?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&useSSL=false&
serverTimezone=GMT%2B8  