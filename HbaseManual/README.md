### Hbase收集器  
Hbase收集器用于离线扫描Hbase数据库表，并将获取的数据记录推送到下游通道；支持字典JSON字串和值序列串两种格式输出。往Hbase数据库中插入数据时，其插入的行键rowKey会被自动排序，并将记录插入行键所在的键序位置，因此Hbase表无法做到运行时期间的实时扫描。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/HbaseManual/dst/hbaseManual.zip -d /install/zip/  
unzip  /install/zip/hbaseManual.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/hbaseManual/source.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|hostList|地址列表|127.0.0.1:2181|连接MongoDB服务的单点地址(单点)或地址列表(集群)|
|targetTab|扫描目标库表|无|本插件运行时需要离线扫描读取的目标库表名称|
|batchSize|批量扫描尺寸|100|本插件离线批量扫描的记录数量，即分页记录尺寸|
|outFormat|输出格式|map|本插件输出格式可选值:seq(字段序列)、map(有序字典)|
|startRowKey|起始行键|空串|指定本插件用于批量扫描的起始行键,即分页起始索引|
|fieldSeparator|字段分隔符|#|当outFormat=seq时，用于指定输出字段序列间的分隔符|


##### 备注：  
1. hostList参数值的多个项之间可以使用英文逗号分隔，若无法识别outFormat参数值时默认使用字段序列输出格式。  
2. targetTab和startRowKey两个参数是必选参数,虽然startRowKey默认值为空串，但实际上行键为空串是无法读取任何数据记录的。  
3. 有序字典输出格式采用JSON字典对象表现层次结构，每条json记录中可以包含多个表的记录，粒度可以达到表级最大化，非常灵活；而字段序列输出格式的每条记录表现的粒度为最小化，即：一个键值为一条记录。  
##### 输出格式举例：  
1. 字段序列输出格式  
格式为: tabble#rowKey#family#key#value，如:  
wa:tuser#001#base#userName#zhangsan
2. 有序字典输出格式  
格式为: {table:{rowKey:{family:{key:value}}}}，如:  
{"wa:tuser":{"001":{"base":{"userName":"zhangsan"}}}}  