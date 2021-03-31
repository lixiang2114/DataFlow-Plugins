### HTTP发送器  
HTTP发送器可以将来自上游通道的数据记录发送到基于HTTP协议的WEB服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。本插件支持通过免密认证、BASE认证或查询参数（表单参数）认证等连接到WEB服务端；同时亦支持通过参数字典、查询字串、JSON消息体或二进制消息流推送数据到WEB服务端。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/HttpSink/dst/httpSink.zip -d /install/zip/  
unzip  /install/zip/httpSink.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/httpSink/sink.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|requireLogin|是否登录|true|使用HTTP协议连接WEB服务端是否需要登录认证|
|authorMode|认证模式|query|requireLogin=true时，认证模式可选值:query、base|
|sendType|发送类型|StreamBody|访问WEB服务器的数据流类型(即:MIME类型)|
|loginURL|登录认证URL|无|requireLogin=true时，登录目标WEB服务器的URL地址|
|postURL|数据推送URL|无|推送数据到目标WEB服务器的URL地址|
|userField|用户名字段名|无|requireLogin=true,authorMode=query时，提交用户名的字段名|
|passField|密码字段名|无|requireLogin=true,authorMode=query时，提交密码的字段名|
|passWord|密码|无|requireLogin=true,authorMode=query时，认证登录密码|
|userName|用户名|无|requireLogin=true,authorMode=query时，认证登录用户名|
|maxRetryTimes|重试次数|3|推送数据到目标WEB服务器失败后的最大重试次数|
|failMaxWaitMills|失败等待|2000|推送数据到目标WEB服务器失败后两次重试之间间隔时间毫秒数|
|messageField|消息字段|无|用于携带发送消息的字段名，若为NULL则直接将发送数据放入消息体|