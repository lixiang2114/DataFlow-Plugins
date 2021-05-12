### HTTP发送器  
HTTP发送器可以将来自上游通道的数据记录发送到基于HTTP协议的WEB服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。在未超过重试次数阈值的情况下未发送成功则累加等待时间并执行循环发送，直到发送成功为止。发送成功的数据将被记录和跟踪检查点位置，这保证了数据刽发生丢失。本插件支持通过免密认证、BASE认证或查询参数（表单参数）认证等连接到WEB服务端；同时亦支持通过参数字典、查询字串、JSON消息体或二进制消息流推送数据到WEB服务端。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/HttpSink/dst/httpSink.zip -d /install/zip/  
unzip  /install/zip/httpSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/httpSink/sink.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|postURL|数据推送URL|无|推送数据到目标WEB服务器的URL地址|
|loginURL|登录认证URL|无|requireLogin=true时，登录目标WEB服务器的URL地址|
|userField|用户名字段名|无|requireLogin=true,authorMode=query时，提交用户名的字段名|
|passField|密码字段名|无|requireLogin=true,authorMode=query时，提交密码的字段名|
|sendType|发送类型|StreamBody|访问WEB服务器的数据流类型(即:MIME类型)|
|passWord|登录密码|无|requireLogin=true,authorMode=query时，认证登录密码|
|userName|登录用户|无|requireLogin=true,authorMode=query时，认证登录用户名|
|lineNumber|行号检查点|0|发送模块退出出执行的行级检查点位置|
|byteNumber|字节检查点|0|发送模块退出出执行的字节检查点位置|
|authorMode|认证模式|base|requireLogin=true时，认证模式可选值:query、base|
|requireLogin|是否登录|true|使用HTTP协议连接WEB服务端是否需要登录认证|
|delOnReaded|删除已读|true|切换缓冲文件时是否删除读取完毕的缓冲文件|
|messageField|消息字段|无|用于携带发送消息的字段名，若为NULL则直接将发送数据放入消息体|
|maxRetryTimes|重试次数|整型最大值|推送数据到目标WEB服务器失败后的最大重试次数|
|failMaxWaitMills|失败等待|0L|推送数据到目标WEB服务器失败后两次重试之间间隔时间毫秒数|
|transferSaveFile|转存缓冲文件|buffer.log.0|本插件使用的磁盘级转存缓冲文件名称|
|maxBatchWaitMills|批次等待时间|15S|拉取上游通道的最大批次等待时间（默认15秒）|
|transferSaveMaxSize|转存最大尺寸|2GB|本插件使用的转存磁盘级缓冲文件的最大尺寸|
|pushOnLoginSuccess|是否即时推送|false|是否在本插件登录远端服务成功后立即推送数据|
##### 备注：   
1. 如果远端服务连接断开，则本插件将捕获一个基于Socket通道的IO异常，并执行循环重试，直到超出设定的重试次数、亦或系统断开插件运行时而终止重试，否则本插件将按等待时间累积值执行循环重试直至成功为止。  
2. 因为本插件支持文件系统缓冲，故无限重试不会导致通道异常，但可能导致日志堆栈信息过多，此情况下可酌情设置一个有限的重试次数以便于中断重试过程。  
3. failMaxWaitMills参数的促使默认值是0，在发生IO异常后将按1秒执行累积等待，这可以尽最大努力减少系统资源占用率。  