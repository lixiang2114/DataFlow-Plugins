### 扩展过滤器  
扩展过滤器用于实现定制化过滤逻辑，即将来自上游通道的数据记录通过自定义的动态脚本实现筛选、过滤、转换并将其发送到下游通道，扩展过滤器支持默认过滤器的透传功能，默认过滤器的透传功能仅过滤NULL、空串和任何空白字符。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/DataFlow-Plugins/raw/main/ExtendFilter/dst/extendFilter.zip -d /install/zip/  
unzip  /install/zip/extendFilter.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/extendFilter/filter.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|through|是否透传|false|设置为true表示透传，可选值:true、false|
|mainClass|入口类名|DefaultClass|through=false时，用于定义动态脚本的入口类全名|
|mainMethod|入口方法|main|through=false时，用于定义动态脚本的入口函数名|
##### 备注：  
through=true时，不会启用自定义的动态脚本，转而直接将记录做简单过滤，然后推送到下游通道；through=false时，将启用自定义的动态脚本来完成数据记录的转换和过滤，动态脚本在本插件被系统调用时装载、编译和执行；自定义的动态脚本支持字串对象返回和字串数组对象返回；请注意，由于本插件支持透传功能，所以本插件覆盖了默认过滤器的所有功能。  