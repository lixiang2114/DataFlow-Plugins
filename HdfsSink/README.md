### HDFS发送器  
HDFS发送器可以将来自上游通道的数据记录存储到Hadoop分布式文件系统中去。当存储数据的文件尺寸超过阈值则以重命名的方式按递增数字序列滚动，插件对名称节点服务支持单节点连接和多节点高可用架构模式连接。由于Hadoop是一种基于磁盘的离线批量分析处理框架，故针对HDFS分布式存储系统，插件采用了本地文件缓冲模式，这可以降低访问HDFS名称节点服务的IO频率，提升数据传输效率。本插件采用字节流IO模式（并非是基于缓冲文件的上传模式），这将有助于尽可能的提升分布式存储数据的实时性。  
      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/HdfsSink/dst/hdfsSink.zip -d /install/zip/  
unzip  /install/zip/hdfsSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/hdfsSink/sink.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|hdfsFile|Hdfs文件绝对路径|无|存储流化ETL数据的初始分布式文件系统路径，该参数为必选参数|
|bufferSize|Hdfs写出缓冲尺寸|4096|由插件推送流化ETL数据到分布式文件存储系统的缓冲尺寸|
|maxHistory|Hdfs文件过期时间|30天|分布式文件系统中文件钝化(未访问)的最大时间，超过该时间则被删除|
|maxFileSize|Hdfs分布式文件尺寸|10GB|存储的目标分布式文件最大尺寸，超过该尺寸按数值序列递增滚动|
|hadoopUser|Hadoop集群操作用户|hadoop|用于操作Hadoop分布式集群的集群例程用户名，通常为hadoop|
|maxBatchBytes|Hdfs批处理文件尺寸|100|本插件通过本地批处理缓冲文件进行缓冲的最大尺寸，单位：字节|

##### 备注：  
1. 目标分布式数据文件仅按尺寸实现滚动记录，与时间无关(因为HDFS是海量级分布式文件存储系统)，这与其它Sink插件有区别。  
2. 该插件中内置etc配置目录，若名称节点NameNode为高可用集群，则通常需要将core-site.xml、hdfs-site.xml、mapred-site.xml和yarn-site.xml四个配置文件同时放入此配置目录，同时还需在hdfsSink.properties主配置文件的classPath属性值中追加etc目录以装载它们。装载这些配置文件并不影响单点NameNode的连接，只要sink.properties配置文件中的hdfs路径反映的主机例程名(可能为ip:port或ns1等)正确即可。  
3. maxBatchBytes参数值越小，实时性越高，但IO频次会增大，资源消耗较快；反之，maxBatchBytes参数值越大，则吞吐量越大，数据传输效率越高，但实时性会随之下降，使用者可根据生产环境酌情优化。  
4. 根据Hadoop架构设计原理，maxFileSize参数值不应过小，maxFileSize参数值越小则磁盘IO和网络IO频率越高，这将增大MapReduce框架在集群中的计算负载，一般认为该参数在保证业务数据文件可维护的前提下尽可能的增大，以减少文件块的数量来保证后续更高效率的分布式计算过程。    