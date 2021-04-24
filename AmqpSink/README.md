### AMQP发送器  
AMQP发送器可以将来自上游通道的数据记录发送到支持AMQP协议（如：RabbitMQ中间件）单点服务器或集群服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。本插件支持多维路由、嵌套路由等复合型拓扑架构定义，亦支持通过免密认证或用户名/密码认证方式连接到Amqp服务端。  
      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/AmqpSink/dst/amqpSink.zip -d /install/zip/  
unzip  /install/zip/amqpSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/amqpSink/sink.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|parse|是否解析|true|默认值true表示解析数据记录为序列，否则按json字串反编译为字典|
|virHost|虚拟主机|无|本插件登录AMQP单点或集群服务器的虚拟主机名称|
|hostList|连接地址列表|127.0.0.1:5672|用于本插件连接AMQP服务端的地址(单点)或地址列表(集群)|
|passWord|登录密码|无|本插件登录AMQP服务器的密码，该参数是必填参数|
|userName|登录用户|无|本插件登录AMQP服务器的用户名，该参数是必填参数|
|routeBinds|路由绑定列表|无|本插件用于定义AMQP服务端路由拓扑架构的路由绑定表|
|fieldSeparator|字段分隔符|#|当parse=true时，本插件用于解析上游通道数据记录的字段分隔符|
|exchangeIndex|交换器索引|0|当parse=true时，指示通道记录中表针目标交换器名称的索引值|
|routingKeyIndex|路由键索引|1|当parse=true时，指示通道记录中表针目标路由键名称的索引值|
|maxRetryTimes|重试次数|3|本插件插入数据记录到AMQP例程服务失败后的最大重试次数|
|failMaxTimeMills|失败等待|2000|插入失败后，前后两次重试之间等待的最大时间间隔(单位:毫秒)|
|exchangeField|交换器字段|exchangeName|当parse=false时，指示通道记录中表针目标交换器名称的字段名|
|routingKeyField|路由键字段|routingKeyName|当parse=false时，指示通道记录中表针目标路由键名称的字段名|
|defaultExchange|默认交换器|defaultExchange|当上游通道记录未指定交换器时，本插件默认使用的交换器名称|
|defaultRoutingKey|默认路由键|defaultRoutingKey|当上游通道记录未指定路由键时，本插件默认使用的路由键名称|

##### 备注：  
1. 对于支持AMQP协议的常用RabbitMQ消息中间件服务器而言，其用户名和密码是必选参数，默认的guest用户只能在本地登录，不能实现远程登录；故用户名和密码参数必须给定  
2. hostList参数值可以有多项，项与项之间使用英文逗号分隔，主机名（或IP地址）与端口之间使用英文冒号分隔即可  
3. routeBinds参数中各项路由绑定使用英文逗号分隔，每项路由绑定中的组件之间使用英文分号分隔，其中组件名称与组件类型（路由键没有组件类型）之间使用英文冒号分隔；每项路由绑定中的第一个组件为AMQP服务器路由的目标组件，该组件可以为队列或交换器类型，第二个组件为AMQP服务器路由的源交换器组件，该组件必须为交换器类型，第三个组件为路由键名称，它没有类型概念并且为可选组件（当源交换器为fanout模式时，路由键可为空）。队列类型只有queue、交换器类型有direct、fanout和topic可选。参数值即为如下格式：  
queue1:queue;exchange1:direct;routKey1,exchange3:direct;exchange2:fanout,queue2:queue;exchange3:direct;routKey2  
4. 约定"交换器名称+路由键名称"被称为支持AMQP协议服务器的复合索引，下同。其中路由键为自定义，原则上只要交换器在AMQP服务中定义，那么AMQP客户端的连接就不会有问题，但不保证发送的数据可以成功推送到AMQP消费者端，因为这还取决于AMQP服务器上的复合索引是否绑定了相应的路由和队列。本插件关于上游通道的输入格式举例如下：  
```Text
1、parse=true输入格式举例:
#推送到默认复合索引
echo "hello1 world..."
#推送到exchange1交换器，该交换器根据routingKey1路由到下一个组件
echo exchange1#routingKey1#hello1 world...

2、parse=false输入格式举例:
#推送到默认复合索引
echo {"message":"hello1 world..."}
#推送到exchange1交换器，该交换器根据routingKey1路由到下一个组件
{"exchangeName":"exchange1","routingKeyName":"routingKey1","message":"hello1 world..."}
```