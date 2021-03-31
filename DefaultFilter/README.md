### 默认过滤器  
默认过滤器用于过滤NULL、空字符串、所有空白字符及不可见字符。如果来自上游通道的数据记录已经处理了这些逻辑，那么默认过滤器等同于直接将上游数据透传到下游通道。默认过滤器没有任何参数配置，因此它无需启动IO读取参数配置文件，这让它表现得相对比较高效和快速，它是被固化的一种过滤模式。  
​      

### 下载与安装  
wget  https://github.com/lixiang2114/DataFlow-Plugins/raw/main/DefaultFilter/dst/defaultFilter.zip -d /install/zip/  
unzip  /install/zip/defaultFilter.zip -d /software/DataFlow-3.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/DataFlow-3.0/plugins/defaultFilter/filter.properties  
​      

### 参数值介绍  
无  