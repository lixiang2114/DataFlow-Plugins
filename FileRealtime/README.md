### 实时文件收集器  
实时文件收集器用于实时扫描缓冲文件或上游通道，将获取的数据记录推送到下游通道。缓冲文件可以是流程中的文件，也可以是流程之外的任何磁盘文件，但必须具备缓冲文件命名规则（xxx.log.0）。实时收集器与离线收集器的区别在于：实时收集器收集的是准实时数据，且为单文件、单线程循环扫描读取，这种扫描机制可通过检查点事务来保证数据的强一致性，从而支持断点续传；当转存的缓冲文件被切换后并不影响收集器的扫描线程，在缓冲文件被读取结束后会自动关闭文件流，同时按数值递增模式自动打开下一个文件流。  
本收集器可以扫描通道数据以实现完全实时的高效数据流传递方案，但通道模式下无法保证数据的强一致性。数据可能因为通道内存泄露导致OOM异常，从而致使数据丢失，因此通道模式并不能应用到对数据安全性较高的场景下。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/FileRealtime/dst/fileRealtime.zip -d /install/zip/  
unzip  /install/zip/fileRealtime.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/fileRealtime/source.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|scanMode|扫描模式|file|收集器扫描模式，可选值:file、channel|
|loggerFile|缓冲文件|buffer.log.0|scanMode=file时，用于定义缓冲文件全名|
|byteNumber|起始字节|0|scanMode=file时，用于定义缓冲文件读取的起始字节|
|lineNumber|起始行号|0|scanMode=file时，用于定义缓冲文件读取的其起始行号|
|delOnReaded|删除缓冲|true|scanMode=file时，用于定义缓冲文件读取结束后是否删除缓冲文件的开关|
##### 备注：  
scanMode=channel表示为通道扫描模式，通道是一种内存数据结构，具备完全实时的数据流传递能力，缺点是不支持事务反馈，无法保证数据的强一致性；loggerFile参数的默认值buffer.log.0所在路径是系统安装目录下flows子目录中对应流程实例目录下的share子目录。  