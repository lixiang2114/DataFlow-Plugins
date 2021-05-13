### 服务转存器  
服务转存器可以将来自第三方服务的实时数据主动拉取并转存到本地缓冲文件系统中去，服务转存器有别于普通的转存器，它主动扫描第三方数据服务，并从服务中获得数据将其缓冲到本地文件系统；本插件同时支持TCP和HTTP的第三方服务协议，TCP扫描依赖于C/S架构的长连接执行实时数据拉取；HTTP依赖于B/S架构的请求/响应执行批量数据拉取。  
如果转存缓冲文件尺寸超过阈值则按数字递增序列滚动，这区别于log4j的重命名方式。虽然转存器支持对下游ETL通道的推送实现，但是考虑到这总机制不利于数据安全，同时易引发通道OOM错误，故现有的转存器实现都是基于转存文件缓冲的，而非通道。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/ServerTransfer/dst/serverTransfer.zip -d /install/zip/  
unzip  /install/zip/serverTransfer.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/serverTransfer/transfer.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|connStr|连接字符串|tcp://127.0.0.1:1567|用于连接第三方数据服务的连接字符串|
|transferSaveFile|转存缓冲文件|buffer.log.0|转存的目标文件，超过阈值尺寸则按数字递增滚动|
|httpPullIntervalMills|拉取间隔时间|5000MS|HTTP协议扫描时，用于设置本插件的数据拉取间隔时间|
|transferSaveMaxSize|转存文件尺寸|2GB|转存目标缓冲文件最大尺寸阈值，超过阈值按数值递增滚动|
##### 备注：  
1. 默认使用的扫描协议是TCP，但支持HTTP协议扫描，TCP协议的扫描效率高于HTTP。  
2. transferSaveFile参数的默认值buffer.log.0所在路径是系统安装目录下flows子目录中对应流程实例目录下的share子目录，目标缓冲文件仅按尺寸实现滚动记录，当尺寸超过设定的阈值将按数字递增序列创建新的目标缓冲文件流，从而完成文件切换操作。