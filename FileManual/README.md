### 离线文件收集器  
离线文件收集器用于离线批量扫描缓冲文件组或上游通道，并将获取的数据记录推送到下游通道，缓冲文件为流程之外的任何磁盘文件。离线收集器与实时收集器的区别在于：实时收集器收集的是准实时数据，且为单文件、单线程循环扫描读取，而本收集器收集的是离线批量数据，且为多文件、多线程批量扫描读取。当一个目录下的所有文件被读取完成后自动断开扫描器，从而致使整个ETL流程自动结束。本收集器支持文件扫描和目录扫描，文件扫描又称为单文件扫描读取，目录扫描又称为多文件批量扫描读取。目录扫描模式下，又分为单线程串行扫描读取和多线程异步并行扫描读取。前者与FileRealtime插件相同，具有检查点和断点续传能力，但效率较低；后者相对效率较高，但不具备断点续传能力。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/FileManual/dst/fileManual.zip -d /install/zip/  
unzip  /install/zip/fileManual.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/fileManual/source.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|scanType|扫描模式|file|离线收集器扫描模式，可选值:file、path|
|lineNumber|起始行号|0|用于检查点记录最后读取文件的结束行号|
|byteNumber|起始字节|0|用于检查点记录最后读取文件的结束字节|
|manualFile|扫描文件|无|scanType=file时，用于定义扫描文件全名|
|manualPath|扫描目录|无|scanType=path时，用于定义扫描目录全路径|
|readedFile|已读文件|readedFiles.ini|scanType=path时，用于保存已读文件全名的文件名|
|multiThread|并发扫描|false|scanType=path时，用于定义是否需要并发扫描的开关|

##### 备注：  
虽然并发扫描模式可以提升读取文件的效率，但是并不利于读取数据的精准和安全，因此我们一般还是使用串行方式扫描读取目录下的每个文件（即：multiThread缺省值为false）；manualFile和manualPath两个参数二选其一即可，但必须至少有一个参数值被显式给出，否则等于放弃使用本插件；readedFile参数的值仅仅指定的是一个不含路径的文件名，该文件的路径为插件运行时目录。  