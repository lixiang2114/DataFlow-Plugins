### Redis转存器  
Redis转存器可以实时拉取Redis管道列表中的数据并将其转存到缓冲文件中去。本插件针对Redis实现的消息中间件并非是基于Redis自身的发布订阅模式，而是基于Redis管道列表数据结构List的阻塞机制来予以实现。之所以没有直接基于Redis的发布订阅模式来实现，是因为Redis作为消息中间件不具备分布式缓存兼容性，因为作为中间件的单节点无法移动（移动之后中间件连接将丢失，因为消息中间件的连接API不支持Redis重定向）；其单节点缓冲尺寸也是很有限的，这容易导致单节点Redis例程发生OOM内存泄露，从而有造成数据丢失的可能。同时作为消息中间件来操作，其客户端API与作为缓存组件的API是不兼容的，这为客户端统一管理Redis操作带来不便；使用管道List数据结构作为中间件缓冲，可以防止数据丢失（只要数据没有被取走，数据将一直存留于Redis缓存中，Redis缓存则可以通过分片扩容，因缓存API支持连接重定向，所以作为中间件的数据管道List所在节点也是可以移动的），同时也可以将其作为缓存API进行统一管理。  
本插件对中间件管道的扫描支持多通道绑定，每次数据拉取将遍历配置中指定多个通道，将多个通道的数据拉取出来写入下游缓冲区。与其它转存器插件类同，如果转存缓冲文件尺寸超过阈值则按数字递增序列滚动，这区别于log4j的重命名方式。虽然转存器支持对下游ETL通道的推送实现，但是考虑到这总机制不利于数据安全，同时易引发通道OOM错误，故现有的转存器实现都是基于转存文件缓冲的，而非通道。  
转存器与收集器的一个最大区别在于：收集器应用的数据源主要是存储或数据库，而转存器应用的数据源主要是来自日志文件、消息中间件或实时服务客户端。前者数据源中的数据是被转存器主动拉取，而后者数据源中的数据是被主动推送给转存器。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/RedisTransfer/dst/redisTransfer.zip -d /install/zip/  
unzip  /install/zip/redisTransfer.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/redisTransfer/transfer.properties  
​      
### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|dbIndex|数据库索引|0|当targetArch=single时，指示在Redis例程中操作的数据库|
|hostList|地址列表|127.0.0.1:6379|连接Redis服务器的单点地址(单点)或地址列表(集群)|
|passWord|Redis密码|无|当Redis例程服务需要登录认证时，指定登录Redis例程的登录密码|
|targetArch|Redis架构|cluster|Redis例程的部署架构，可选值：single(单点)、cluster(集群)|
|targetPipes|管道列表|all|存储管道对象列表，只能是List结构，多个管道名之间用英文逗号分隔|
|maxRedirect|最大重定向|集群节点数-1|当targetArch=cluster时，指示为键寻找目标节点的最大重定向次数|
|pollTimeoutMills|批次超时|100|本插件客户端每次扫描拉取数据时等待的最大超时时间(单位:毫秒)|
|transferSaveFile|转存文件|buffer.log.0|转存的目标缓冲文件，目标缓冲文件超过阈值尺寸则按数字递增滚动|
|transferSaveMaxSize|转存尺寸|2GB|本插件转存目标缓冲文件的最大阈值尺寸，目标缓冲文件尺寸单位可选|


##### 备注：  
transferSaveFile参数的默认值buffer.log.0所在路径是系统安装目录下flows子目录中对应流程实例目录下的share子目录，目标数据文件仅按尺寸实现滚动记录，当尺寸超过设定的阈值将按数字递增序列创建新的目标数据文件流，从而完成文件切换操作。