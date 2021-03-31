### Elastic收集器  
Elastic收集器用于离线读取Elasticsearch索引库中的数据，并将获取的数据记录推送到下游通道。支持免密登录和用户名/密码登录；支持字典JSON字串、查询字串和值序列串三种格式输出。由于Elasticsearch的全文检索过程会自动根据_score排序，这导致本插件无法依赖于分页SQL实时扫描到最新数据，故本插件目前只能读取Elasticsearch的离线数据集。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/ElasticSource/dst/elasticSource.zip -d /install/zip/  
unzip  /install/zip/elasticSource.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/elasticSource/source.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|realtime|是否实时|true|是否实时读取，即便为true也只支持离线读取|
|hostList|地址列表|127.0.0.1:9200|连接Elastic服务的单点地址或地址列表|
|timeZone|时区差值|+0800|时间字段与格林威治时间差，默认为东八区|
|timeFields|时间字段|无|索引库中需要转换为可读字串的时间字段列表|
|selectSQL|分页SQL|{"query":{"match_all":{}},"from":0,"size":100}|离线读取Elasticsearch服务的分页SQL语句|
|outFormat|输出格式|qstr|格式可选值:qstr(查询字串)、map(json字串)|
|userName|登录用户|无|登录用户，用户名或密码为NULL启用免密登录|
|passWord|登录密码|无|登录密码，用户名或密码为NULL启用免密登录|
|indexName|索引库名|无|本插件读取的索引库名称，该参数是必选参数|
|clusterName|集群名称|无|某些客户端连接Elasticsearch集群需要名称|
|enableAuthCache|缓存认证|无|登录认证时，是否缓存认证的用户名和密码|
##### 备注：  
hostList参数的多个值之间使用英文逗号分隔，若无法识别outFormat参数值则默认使用的输出格式为值序列格式。  