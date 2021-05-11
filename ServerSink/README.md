### 服务发送器  
服务发送器又称为反向服务推送器，它可以将来自上游通道的数据记录通过TCP（基于C/S的长连接）或HTTP协议（基于Web的请求响应架构）服务反向发送到基于TCP或HTTP协议的客户端服务器上。由于上游通道的数据首先被缓冲到该插件所在的文件系统存储上，然后再通过缓冲扫描的方式将其拾起发送到HTTP或TCP协议的客户端上（客户端可以基于HTTP或TCP协议扫描本插件提供的对应服务）；插件实时记录发送的检查点位置，如果发送失败，则后续将基于检查点位置开始重新发送，这保证了数据不会发生丢失。本插件支持HTTP协议或TCP协议的客户端通过免密认证或用户名/密码认证方式连接到本插件服务端。  
      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/ServerSink/dst/serverSink.zip -d /install/zip/  
unzip  /install/zip/serverSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/serverSink/sink.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|port|服务端口|1567|反向推送服务端口|
|protocol|服务协议|TCP|反向推送服务协议，可选值有：TCP，HTTP|
|recvType|接收类型|MessageBody|可选值:ParamMap、QueryString、StreamBody、MessageBody|
|passField|用户字段|无|requireLogin=true时，用于表示登录提交的密码字段名称|
|userField|密码字段|无|requireLogin=true时，用于表示登录提交的用户名字段名称|
|passWord|登录密码|无|requireLogin=true时，用于表示登录本插件的密码参数值|
|userName|登录用户|无|requireLogin=true时，用于表示登录本插件的用户名参数值|
|authorMode|认证模式|auto|requireLogin=true时，认证模式可选值:query、base、auto|
|lineNumber|行号检查点|0|反向推送服务对缓冲文件行号的检查点|
|byteNumber|字节检查点|0|反向推送服务对缓冲文件字节位置的检查点|
|lineSeparator|输出分隔符|\n|反向推送服务数据输出分隔符|
|keepSession|是否保持会话|false|是否为HTTP协议的服务保持会话连续|
|delOnReaded|删除已读缓冲|true|是否自动删除已经读取完毕的缓冲文件|
|requireLogin|是否需要登录|false|客户端扫描本插件服务拉取数据前是否需要登录|
|loginFailureId|登录失败反馈|NO|requireLogin=true时，WEB客户端登录失败后接收到的ACK消息|
|loginSuccessId|登录成功反馈|OK|requireLogin=true时，WEB客户端登录成功后接收到的ACK消息|
|transferSaveFile|转存缓冲文件|buffer.log.0|用于转存实时消息记录的缓冲文件名|
|httpBatchSendSize|批量发送尺寸|100|HTTP服务批量发送记录的数量|
|maxBatchWaitMills|批次等待时间|15000|缓冲服务拉取上游通道数据的最大等待时间|
|transferSaveMaxSize|转存文件尺寸|2GB|转存实时消息记录的缓冲文件最大尺寸|
|pushOnLoginSuccess|单段即时推送|true|是否在登录成功之后立即推送数据|
##### 备注：  
1. 针对TCP协议而言，keepSession参数保持的是进程级别的全局会话。  
2. recvType为HTTP服务接收的MIME类型，仅在protocol=HTTP时有效。  
3. 本插件主要应用到第三方通过服务模式主动扫描获取数据的场景，如：Spark-Streaming
4. 通常情况下，TCP协议可能会拥有比HTTP协议更高的数据传输效率，但TCP传输大多应用到内网 
5. 在内网传输数据，请将登录开关参数requireLogin设置为false，同时将会话保持参数keepSession设置为false，这将忽略每次请求的登录检查逻辑；同时不会维护客户端的会话状态，从而可在内网获得更高层级的数据传输效率
6. TCP协议模式下，数据传输依赖于长连接实现，其会话是全局的，即所有客户端共享同一个会话，请不要依赖于TCP传输协议设计任何会话隔离逻辑；HTTP协议模式下，数据传输依赖于Web请求响应模式实现，客户端的每次请求将被回应一个设定好的最大批次数据量，其会话是客户端隔离级别，这允许多个客户端并发拉取本插件的反向服务数据。   