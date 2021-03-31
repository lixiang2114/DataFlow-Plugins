### 文件发送器  
文件发送器可以将来自上游通道的数据记录存储到本地文件系统中去。当存储数据的文件尺寸或创建时间超过阈值则以重命名的方式按时间和递增数字执行滚动，此特征与日志框架log4j是类同的。每当系统调用以启动本插件时，该插件都会检查存储数据的文件所在目录中的历史数据文件是否过期，如果过期则执行历史数据文件的删除操作。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/FileSink/dst/fileSink.zip -d /install/zip/  
unzip  /install/zip/fileSink.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/fileSink/sink.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|filePath|文件路径|无|目标数据文件的绝对路径，后续将以此文件为种子实现滚动记录|
|maxHistory|保留时间|1d|历史目标文件最大过期时间，超过该时间的目标文件将被自动删除|
|maxFileSize|文件尺寸|100MB|发送目标文件最大尺寸，超过该尺寸，文件名按日期和数字递增滚动|
##### 备注：  
目标数据文件按日期和尺寸实现滚动记录，当日期翻天或尺寸超过阈值都将造成数据文件被重命名切换，翻天切换的文件名被重命名时将修改其日期，尺寸超过阈值切换的文件名将递增最后的数字序列。