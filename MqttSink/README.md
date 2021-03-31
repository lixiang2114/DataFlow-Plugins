### MQTT发送器  
MQTT发送器可以将来自上游通道的数据记录发送到基于MQTT协议的消息中间件服务器上。当因为网络故障造成数据发送失败时，将自动根据设定的重试次数阈值重新发送失败的数据。如果超过重试次数阈值还未发送成功则自动记录发送失败的数据到排重集合中，待下次发送数据前将首先发送上次发送失败的数据记录。本插件支持通过免密认证、证书认证、Token认证和用户名/密码认证等方式连接到关系数据库服务端。  
      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/MqttSink/dst/mqttSink.zip -d /install/zip/  
unzip  /install/zip/mqttSink.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/mqttSink/sink.properties  
      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|qos|通信等级|1|用于衡量通信质量好坏程度的等级指标|
|parse|是否解析|true|true表示解析记录为字典，否则按json字串反序列化为字典|
|hostList|地址列表|127.0.0.1:1883|连接MQTT服务器的单点地址(单点)或地址列表(集群)|
|jwtSecret|Token秘钥|无|Token认证时的秘钥，是否可以匿名访问取决于服务器类型|
|passWord|登录密码|public|本插件登录到MQTT消息中间件服务器的认证用户密码|
|userName|登录用户|admin|本插件登录MQTT消息中间件服务器的认证用户名|
|tokenFrom|Token字段|password|本插件登录认证MQTT消息中间件服务时，携带token的字段|
|rootCaFile|根证书路径|无|本插件启用证书认证时的根证书路径|
|topicField|主题字段|topic|parse=false时，用于从记录字典提取主题名称的字段名称|
|topicIndex|主题索引|0|parse=true时，用于解析主题名称的记录字段索引|
|tokenExpire|Token时长|1h|本插件客户端持有的Token的过期时长，-1表示永不过期|
|clientCaFile|客户端证书|无|本插件启用证书认证时的客户端证书路径|
|clientKeyFile|客户端秘钥|无|本插件启用证书认证时的客户端证书秘钥|
|cleanSession|清除会话|true|本插件断开MQTT消息中间件服务器后是否清除会话信息|
|expireFactor|过期因子|750/1000|过期时间达到多少倍率开始更新过期时间|
|defaultTopic|默认主题|无|当数据记录中无法解析或提取主题名称时使用该默认值|
|protocolType|连接协议|tcp|本插件连接MQTT消息中间件服务使用的传输层协议类型|
|fieldSeparator|字段分隔符|,|parse=true时，用于解析上游通道记录的字段分隔符|
|maxRetryTimes|重试次数|3|推送数据记录到SQL关系数据库失败后的最大重试次数|
|persistenceType|持久类型|MemoryPersistence|本插件客户端对MQTT消息中间件中消息的持久化类型|
|failMaxWaitMills|失败等待|2000|推送数据失败后，前后两次重试的时间间隔(单位:毫秒)|
|keepAliveInterval|心跳间隔|60s|本插件与MQTT服务端保持长连接的心跳时间间隔(单位:秒)|
|clientCaPassword|客户端证书密码|无|本插件启用客户端证书认证时输入的证书密码|
|connectionTimeout|下发超时|30s|本插件连接MQTT服务器的超时时间(单位:秒)|
|automaticReconnect|自动重连|true|本插件断开MQTT服务器后是否自动重新连接|
##### 备注：  
1. MQTT服务器是否可以被匿名访问取决于服务器的类型，如果服务器支持则可以设置为免密登录模式。
2. 当tokenExpire为NULL时，expireFactor默认值为750，当tokenExpire为-1时，expireFactor默认值为1000。
3. MemoryPersistence类的全类名是：org.eclipse.paho.client.mqttv3.persist.MemoryPersistence.MemoryPersistence，除了可以基于内存持久化，可选的持久化方式还有磁盘。
4. fieldSeparator参数的默认值是英文逗号，一般针对消息中间件的Sink插件只需要区分主题字段即可，并且以记录中首次出现的分隔符为准来分隔主题字段，所以默认使用英文逗号即可；而对于针对存储或数据库的Sink插件由于需要区分过多的机构化字段，故默认使用空白字符。  