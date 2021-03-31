### 流程发送器  
流程发送器可以将本插件所在流程上游通道数据推送到一组目标流程或存储文件，目标介质可以是流程实例或磁盘文件。若推送的目标介质是流程实例，则流程又可分为缓冲和通道。通道是基于内存传输的一种数据结构，优点是高实时、高效率、速度快；缺点是不具备事务反馈能力、无法保证数据一致性，易造成内存泄露和数据丢失。  
本插件支持同步复制、负载均衡、条件推送及自定义算法等转发策略。其中负载均衡算法包括随机、轮训和哈希；条件推送模式下可根据记录字段值满足的条件将其推送到不同的目标组；自定义算法支持动态脚本配置、记录行过滤和自定义目标索引生成策略。  
该插件是系统框架用于实现多维流程、复合流程和嵌套流程的核心类插件，若业务中涉及到复杂流程分支则必须基于该插件来构建闭环流程以实现嵌套分支逻辑。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/FlowSink/dst/flowSink.zip -d /install/zip/  
unzip  /install/zip/flowSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/flowSink/sink.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|sendMode|转发模式|rep|可选值:rep、field、custom、hash、robin、random|
|targetFileMaxSize|文件尺寸|2GB|转存目标文件最大尺寸，超过该尺寸，文件名按数字递增滚动|
|dispatcherItems|转发目标项|无|转发列表为一到多个使用英文逗号分隔的项，目标可以是磁盘文件或流程实例|
|rule|键字规则|index|sendMode=field时，用于查找目标项的记录行键字规则，可选值:index、key|
|key|目标键字|flows|rule=key时，用于查找目标项的键字值|
|index|目标索引|0|rule=index时，用于查找目标项的索引值|
|itemSeparator|项目分隔符|#|sendMode=field时，用于分隔目标项的分隔符|
|fieldSeparator|字段分隔符|,|rule=index时，用于分隔记录中字段的分隔符|
|mainClass|入口类名|DefaultClass|sendMode=custom时，用于定义动态脚本的入口类全名|
|mainMethod|入口方法|main|sendMode=custom时，用于定义动态脚本的入口函数名|
##### 备注：  
dispatcherItems参数值的格式为:  [targetType01:]item01,[targetType02:]item02...  
其中targetType是可选的，缺省值为：buffer，可选targetType值有：file、buffer、channel