### HTTP转存器  
HTTP转存器可以将来自WEB客户端程序主动推送的数据转存到缓冲文件中去，如果转存缓冲文件尺寸超过阈值则按数字递增序列滚动，这区别于log4j的重命名方式。虽然转存器支持对下游ETL通道的推送实现，但是考虑到这总机制不利于数据安全，同时易引发通道OOM错误，故现有的转存器实现都是基于转存文件缓冲的，而非通道。  
本转存器是基于高性能NIO框架Netty实现的，是系统框架用于对接远程服务组件的核心类插件。该插件对WEB客户端支持免密认证、BASE认证、查询参数（表单参数）认证和自动认证等；消息传输模式包括查询参数、参数字典、二进制流和JSON消息体等。其中绑定端口、认证反馈消息和转存缓冲文件尺寸等均可通过配置自定义。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/HttpTransfer/dst/httpTransfer.zip -d /install/zip/  
unzip  /install/zip/httpTransfer.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/httpTransfer/transfer.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|port|服务端口|8080|HTTP服务端绑定的WEB端口，默认绑定所有网卡IP地址|
|recvType|接收类型|MessageBody|类型可选值:ParamMap、QueryString、StreamBody、MessageBody|
|requireLogin|是否登录|true|WEB客户端访问本插件绑定的WEB服务是否需要登录|
|userName|用户名|无|requireLogin=true时，用于表示提交的密码参数值|
|passWord|密码|无|requireLogin=true时，用于表示提交的用户名参数值|
|userField|密码字段|无|requireLogin=true时，用于表示提交的密码字段名称|
|passField|用户名字段|无|requireLogin=true时，用于表示提交的用户名字段名称|
|errorReply|异常响应|NO|WEB客户端访问本插件服务发生异常时的ACK响应值|
|normalReply|正常响应|OK|WEB客户端访问本插件服务正常时的ACK响应值|
|authorMode|认证模式|auto|requireLogin=true时，认证模式可选值:query、base、auto|
|loginFailureId|登录失败反馈|NO|requireLogin=true时，WEB客户端登录失败后接收到的ACK消息|
|loginSuccessId|登录成功反馈|OK|requireLogin=true时，WEB客户端登录成功后接收到的ACK消息|
|transferSaveFile|转存缓冲文件|buffer.log.0|用于转存实时消息记录的缓冲文件名|
|transferSaveMaxSize|转存文件尺寸|2GB|转存实时消息记录的缓冲文件最大尺寸|
##### 备注：  
认证模式authorMode参数值为auto指的是首先基于查询参数认证，当查询参数认证未通过时自动启用BASE认证，若BASE认证也未通过则认为客户端登录失败；transferSaveFile参数的默认值buffer.log.0所在路径是系统安装目录下flows子目录中对应流程实例目录下的share子目录；缓冲数据文件仅按尺寸实现滚动记录，当尺寸超过设定的阈值将按数字递增序列创建新的缓冲数据文件流，从而完成文件切换操作。