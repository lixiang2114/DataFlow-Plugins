### KafKa转存器  
KafKa转存器可以实时拉取KafKa消息中间件服务的数据并将其转存到缓冲文件中去，转存器与收集器的一个最大区别在于：收集器应用的数据源主要是存储或数据库，而转存器应用的数据源主要是来自日志文件、消息中间件或实时服务客户端。前者数据源中的数据是被转存器主动拉取，而后者数据源中的数据是被主动推送给转存器。  
如果转存缓冲文件尺寸超过阈值则按数字递增序列滚动，这区别于log4j的重命名方式。虽然转存器支持对下游ETL通道的推送实现，但是考虑到这总机制不利于数据安全，同时易引发通道OOM错误，故现有的转存器实现都是基于转存文件缓冲的，而非通道。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/KafkaTransfer/dst/kafkaTransfer.zip -d /install/zip/  
unzip  /install/zip/kafkaTransfer.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/kafkaTransfer/transfer.properties  
​      
### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|topic|连接主题|无|连接KafKa服务器的主题名称，该参数是必选参数|
|hostList|地址列表|127.0.0.1:9092|连接KafKa服务器的单点地址(单点)或地址列表(集群)|
|transferSaveFile|转存文件|buffer.log.0|转存的目标文件，超过阈值尺寸则按数字递增滚动|
|autoCreateTopic|自创主题|true|当KafKa服务器上没有访问的主题时是否自动创建主题|
|consumerGroupId|消费组ID|lixiang2114|本插件作为KafKa中间件服务的消费者客户端的组ID|
|sessionTimeoutMills|会话超时|30000|本插件客户端与KafKa服务端的会话超时时间(单位:毫秒)|
|transferSaveMaxSize|转存尺寸|2GB|本插件转存目标文件的最大阈值尺寸，单位可选|
|batchPollTimeoutMills|批次超时|100|本插件客户端等待每一个批次的最大超时时间(单位:毫秒)|
|partitionNumPerTopic|分区数量|1|每个主题的默认分区数量，分区用于表针并行度的高低|
|autoCommitIntervalMills|提交间隔|1000|本插件客户端前后两次提交事务的时间间隔(单位:毫秒)|
##### 备注：  
transferSaveFile参数的默认值buffer.log.0所在路径是系统安装目录下flows子目录中对应流程实例目录下的share子目录，目标数据文件仅按尺寸实现滚动记录，当尺寸超过设定的阈值将按数字递增序列创建新的目标数据文件流，从而完成文件切换操作。