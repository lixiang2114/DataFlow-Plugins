package com.df.plugin.source.elastic.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Elastic客户端配置
 */
@SuppressWarnings("unchecked")
public class ElasticConfig {
	/**
	 * 当前流程实例对象
	 */
	public Flow flow;
	
	/**
	 * 收集器运行时路径
	 */
	public File sourcePath;
	
	/**
	 * 时区差值
	 */
	public String timeZone;
	
	/**
	 * 是否为实时读取
	 */
	public Boolean realtime;
	
	/**
	 * Elastic服务器用户
	 */
	public String userName;
	
	/**
	 * Elastic服务器密码
	 */
	public String passWord;
	
	/**
	 * 索引库名称
	 */
	public String indexName;
	
	/**
	 * 输出数据格式
	 * qstr: 查询字串格式 
	 * map: 字典Json格式
	 */
	public String outFormat;
	
	/**
	 * SQL客户端配置
	 */
	public Properties config;
	
	/**
	 * 分页起始索引
	 */
	public Integer startIndex;
	
	/**
	 * 页面记录数(批处理尺寸)
	 */
	public Integer batchSize;
	
	/**
	 * 集群名称
	 */
	public String clusterName;
	
	/**
	 * 主机地址表
	 */
	private HttpHost[] hostList;
	
	/**
	 * ES集群客户端
	 */
	public RestClient restClient;
	
	/**
	 * 读出Elastic数据库的时间字段集
	 * 通常为java.util.Date的子类型
	 */
	public Set<String> timeFieldSet;
	
	/**
	 * 是否可用认证缓存(默认可用)
	 */
	public boolean enableAuthCache=true;
	
	/**
	 * 查询参数字典
	 */
	public HashMap<String,Object> queryParamDict;
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	private static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ElasticConfig.class);
	
	/**
     * IP地址正则式
     */
	private static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	public ElasticConfig(){}
	
	public ElasticConfig(Flow flow) {
		this.flow=flow;
		this.sourcePath=flow.sourcePath;
		this.config=PropertiesReader.getProperties(new File(sourcePath,"source.properties"));
	}
	
	/**
	 * @param config
	 */
	public ElasticConfig config() {
		String indexNameStr=config.getProperty("indexName","").trim();
		if(indexNameStr.isEmpty()) throw new RuntimeException("No Index Name Specified...");
		
		this.indexName="/"+indexNameStr.toLowerCase();
		
		initHostAddress();
		initSelectSqlParameter();
		
		String timeZoneStr=config.getProperty("timeZone","").trim();
		this.timeZone=timeZoneStr.isEmpty()?"+0800":timeZoneStr;
		
		String outFormatStr=config.getProperty("outFormat","").trim();
		this.outFormat=outFormatStr.isEmpty()?"qstr":outFormatStr;
		
		String realtimeStr=config.getProperty("realtime","").trim();
		this.realtime=realtimeStr.isEmpty()?true:Boolean.parseBoolean(realtimeStr);
		
		String clusterNameStr=config.getProperty("clusterName","").trim();
		this.clusterName=clusterNameStr.isEmpty()?null:clusterNameStr;
		
		String enableAuthCacheStr=config.getProperty("enableAuthCache","").trim();
		if(!enableAuthCacheStr.isEmpty()) enableAuthCache=Boolean.parseBoolean(enableAuthCacheStr);
		
		String timeFieldStr=config.getProperty("timeFields","").trim();
		this.timeFieldSet=timeFieldStr.isEmpty()?new HashSet<String>():Arrays.stream(COMMA_REGEX.split(timeFieldStr)).map(e->e.trim()).collect(Collectors.toSet());
		
		String passWordStr=config.getProperty("passWord","").trim();
		String userNameStr=config.getProperty("userName","").trim();
		if(!passWordStr.isEmpty() && !userNameStr.isEmpty()) {
			userName=userNameStr;
			passWord=passWordStr;
		}
		
		RestClientBuilder builder=RestClient.builder(hostList);
		builder.setDefaultHeaders(new Header[]{new BasicHeader("Content-Type","application/json;charset=UTF-8")});
		
		if(null!=userName && null!=passWord) {
			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,passWord));
			
			builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
		        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
		        	if(!enableAuthCache)httpClientBuilder.disableAuthCaching();
		            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		        }
		    });
		}
		
		restClient=builder.build();
		
		return this;
	}
	
	/**
	 * 初始化查询SQL参数
	 */
	private void initSelectSqlParameter() {
		String selectSQLStr=config.getProperty("selectSQL", "").trim();
		String selectSQL=selectSQLStr.isEmpty()?"{\"query\":{\"match_all\":{}},\"from\":0,\"size\":100}":selectSQLStr;
		if(!selectSQL.startsWith("{") || !selectSQL.endsWith("}")) {
			log.error("elastic sql must be dictionary json format: {....}");
			throw new RuntimeException("elastic sql must be dictionary json format: {....}");
		}
		
		HashMap<String,Object> queryDict=CommonUtil.jsonStrToJava(selectSQL, HashMap.class);
		Number startIndexNum=(Number)queryDict.get("from");
		Number batchSizeNum=(Number)queryDict.get("size");
		
		if(null==startIndexNum || null==batchSizeNum) {
			log.error("last two params must be from and size");
			throw new RuntimeException("last two params must be from and size");
		}
		
		this.queryParamDict=queryDict;
		this.batchSize=batchSizeNum.intValue();
		this.startIndex=startIndexNum.intValue();
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String hostListStr=config.getProperty("hostList", "").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:9200"}:COMMA_REGEX.split(hostListStr);
		hostList=new HttpHost[hosts.length];
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(host.isEmpty()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList[i]=new HttpHost(ip, Integer.parseInt(port), "http");
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList[i]=new HttpHost("127.0.0.1", Integer.parseInt(unknow), "http");
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList[i]=new HttpHost(unknow, 9200, "http");
			}
		}
	}
	
	/**
	 * 刷新数据表记录检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		OutputStream fos=null;
		config.setProperty("selectSQL",CommonUtil.javaToJsonStr(queryParamDict));
		try{
			fos=new FileOutputStream(new File(sourcePath,"source.properties"));
			log.info("reflesh checkpoint...");
			config.store(fos, "reflesh checkpoint");
		}finally{
			if(null!=fos) fos.close();
		}
	}
	
	/**
	 * 获取字段值
	 * @param key 键
	 * @return 返回字段值
	 */
	public Object getFieldValue(String key) {
		if(null==key) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=ElasticConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Object fieldVal=field.get(this);
			Class<?> fieldType=field.getType();
			if(!fieldType.isArray()) return fieldVal;
			
			int len=Array.getLength(fieldVal);
			StringBuilder builder=new StringBuilder("[");
			for(int i=0;i<len;builder.append(Array.get(fieldVal, i++)).append(","));
			if(builder.length()>1) builder.deleteCharAt(builder.length()-1);
			builder.append("]");
			return builder.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 设置字段值
	 * @param key 键
	 * @param value 值
	 * @return 返回设置后的值
	 */
	public Object setFieldValue(String key,Object value) {
		if(null==key || null==value) return null;
		String attrName=key.trim();
		if(attrName.isEmpty()) return null;
		
		try {
			Field field=ElasticConfig.class.getDeclaredField(attrName);
			field.setAccessible(true);
			
			Class<?> fieldType=field.getType();
			Object fieldVal=null;
			if(String[].class==fieldType){
				fieldVal=COMMA_REGEX.split(value.toString());
			}else if(File.class==fieldType){
				fieldVal=new File(value.toString());
			}else if(CommonUtil.isSimpleType(fieldType)){
				fieldVal=CommonUtil.transferType(value, fieldType);
			}else{
				return null;
			}
			
			field.set(this, fieldVal);
			return value;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 收集实时参数
	 * @return
	 */
	public String collectRealtimeParams() {
		HashMap<String,Object> map=new HashMap<String,Object>();
		map.put("realtime", realtime);
		map.put("batchSize", batchSize);
		map.put("passWord", passWord);
		map.put("startIndex", startIndex);
		map.put("userName", userName);
		map.put("outFormat", outFormat);
		map.put("sourcePath", sourcePath);
		map.put("indexName", indexName);
		map.put("timeFieldSet", timeFieldSet);
		map.put("clusterName", clusterName);
		map.put("selectSQL", queryParamDict);
		map.put("hostList", Arrays.toString(hostList));
		map.put("enableAuthCache", enableAuthCache);
		return map.toString();
	}
}
