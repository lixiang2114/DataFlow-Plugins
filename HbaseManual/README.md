### Hbase收集器  
Hbase收集器用于近实时扫描Hbase数据库表，并将获取的数据记录推送到下游通道；支持字典JSON字串和值序列串两种格式输出。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/MongoSource/dst/mongoSource.zip -d /install/zip/  
unzip  /install/zip/mongoSource.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/mongoSource/source.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|realtime|是否实时|true|是否为实时扫描模式，本插件支持近实时扫描模式|
|hostList|地址列表|127.0.0.1:27017|连接MongoDB服务的单点地址(单点)或地址列表(集群)|
|selectSQL|分页SQL|[{$skip:0},{$limit:100}]|近实时扫描MongoDB数据库表的分页SQL语句|
|outFormat|输出格式|qstr|输出格式可选值:qstr(查询字串)、map(json字串)|
|userName|登录用户|无|登录用户名，用户名或密码为NULL则启用免密登录|
|passWord|登录密码|无|登录密码，用户名或密码为NULL则启用免密登录|
|connectTimeout|下发超时|30000|连接MongoDB服务器的最大超时时间(单位:毫秒)|
|dataBaseName|数据库|无|扫描的MongoDB数据库名称，对Source插件而言，本参数是必选参数|
|collectionName|数据表|无|扫描的MongoDB数据表名称，对Source插件而言，本参数是必选参数|
##### 备注：  
hostList参数值的多个项之间可以使用英文逗号分隔，若无法识别outFormat参数值时默认使用的输出格式为值序列格式。另外本插件还支持很多相关MongoDB的连接参数项，这些参数项的名称与MongoDB官方参数名相同，有兴趣者可以自行测试之。  