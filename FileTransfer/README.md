### 文件转存器  
文件转存器可以将来自文件系统的实时数据主动推送并转存到缓冲文件中去，转存器与收集器的一个最大区别在于：收集器应用的数据源主要是存储或数据库，而转存器应用的数据源主要是来自日志文件、消息中间件或实时服务客户端。前者数据源中的数据是被转存器主动拉取，而后者数据源中的数据是被主动推送给转存器。  
如果转存缓冲文件尺寸超过阈值则按数字递增序列滚动，这区别于log4j的重命名方式。虽然转存器支持对下游ETL通道的推送实现，但是考虑到这总机制不利于数据安全，同时易引发通道OOM错误，故现有的转存器实现都是基于转存文件缓冲的，而非通道。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/FileTransfer/dst/fileTransfer.zip -d /install/zip/  
unzip  /install/zip/fileTransfer.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/fileTransfer/transfer.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|appLogFile|源文件|无|扫描的目标源文件|
|transferSaveMaxSize|转存尺寸|2GB|转存目标文件最大阈值尺寸|
|transferSaveFile|转存文件|buffer.log.0|转存的目标文件，超过阈值尺寸则按数字递增滚动|
##### 备注：  
transferSaveFile参数的默认值buffer.log.0所在路径是系统安装目录下flows子目录中对应流程实例目录下的share子目录，目标数据文件仅按尺寸实现滚动记录，当尺寸超过设定的阈值将按数字递增序列创建新的目标数据文件流，从而完成文件切换操作。