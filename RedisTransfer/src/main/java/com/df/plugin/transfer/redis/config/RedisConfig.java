package com.df.plugin.transfer.redis.config;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.df.plugin.transfer.redis.dto.Host;
import com.df.plugin.transfer.redis.util.RedisUtil;
import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description Redis客户端配置
 */
@SuppressWarnings("unchecked")
public class RedisConfig {
	/**
	 * 流程实例
	 */
	public Flow flow;
	
	/**
	 * 数据库索引
	 */
	public Integer dbIndex;
	
	/**
	 * 扫描的目标管道
	 */
	public String[] targetPipes;
	
	/**
	 * 插件运行时路径
	 */
	public File pluginPath;
	
	/**
	 * Redis服务器密码
	 */
	public String passWord;
	
	/**
	 * 最大重定向次数
	 */
	public Integer maxRedirect;
	
	/**
	 * 目标架构类型
	 */
	public RedisArch targetArch;
	
	/**
	 * 转存目录
	 */
	public File transferPath;
	
	/**
	 * Redis客户端配置
	 */
	private Properties config;
	
	/**
	 * Redis客户端工具
	 */
	public RedisUtil redisUtil;
	
	/**
	 * 实时转存的日志文件
	 */
	public File transferSaveFile;
	
	/**
	 * 拉取超时时间(单位:毫秒)
	 */
	public Long pollTimeoutMills;
	
	/**
	 * 转存日志文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * Redis主机列表
	 */
	private ArrayList<Host> hostList=new ArrayList<Host>();
	
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
	public static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(RedisConfig.class);
	
	/**
	 * 默认序列化器
	 */
	private RedisSerializer<String> defaultSerializer=new StringRedisSerializer();
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public RedisConfig(){}
	
	public RedisConfig(Flow flow){
		this.flow=flow;
		this.transferPath=flow.sharePath;
		this.pluginPath=flow.transferPath;
		this.config=PropertiesReader.getProperties(new File(pluginPath,"transfer.properties"));
	}
	
	/**
	 * @param config
	 */
	public RedisConfig config() {
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
		if(transferSaveFileName.isEmpty()) {
			transferSaveFile=new File(transferPath,"buffer.log.0");
			log.warn("not found parameter: 'transferSaveFile',will be use default...");
		}else{
			File file=new File(transferSaveFileName);
			if(!file.exists() || file.isFile()) transferSaveFile=file;
		}
		
		if(null==transferSaveFile) {
			log.error("transferSaveFile can not be NULL...");
			throw new RuntimeException("transferSaveFile can not be NULL...");
		}
		
		log.info("transfer save logger file is: "+transferSaveFile.getAbsolutePath());
		
		transferSaveMaxSize=getTransferSaveMaxSize();
		log.info("transfer save logger file max size is: "+transferSaveMaxSize);
		
		initHostAddress();
		
		if(hostList.isEmpty()) {
			log.error("hostList parameter must be specify...");
			throw new RuntimeException("hostList parameter must be specify...");
		}
		
		String passWordStr=config.getProperty("passWord","").trim();
		this.passWord=passWordStr.isEmpty()?null:passWordStr;
		
		String targetPipeStr=config.getProperty("targetPipes","").trim();
		this.targetPipes=targetPipeStr.isEmpty()?new String[]{"all"}:COMMA_REGEX.split(targetPipeStr);
		for(int i=0;i<targetPipes.length;targetPipes[i]=targetPipes[i++].trim());
		
		String dbIndexStr=config.getProperty("dbIndex","").trim();
		this.dbIndex=dbIndexStr.isEmpty()?0:Integer.parseInt(dbIndexStr);
		
		String pollTimeoutMillStr=config.getProperty("pollTimeoutMills","").trim();
		this.pollTimeoutMills=pollTimeoutMillStr.isEmpty()?100:Long.parseLong(pollTimeoutMillStr);
		
		String targetArchStr=config.getProperty("targetArch","").trim();
		this.targetArch=targetArchStr.isEmpty()?RedisArch.cluster:RedisArch.valueOf(targetArchStr);
		
		String maxRedirectStr=config.getProperty("maxRedirect","").trim();
		this.maxRedirect=maxRedirectStr.isEmpty()?(hostList.size()-1):Integer.parseInt(maxRedirectStr);
		
		this.redisUtil=new RedisUtil(getRedisTemplate());
		
		return this;
	}
	
	/**
	 * 获取Spring-Redis模板工具
	 * @return Redis模板工具
	 */
	private RedisTemplate<String,Object> getRedisTemplate() {
		RedisTemplate<String, Object> redisTemplate=new RedisTemplate<String, Object>();
		RedisConnectionFactory connectionFactory=getRedisConnectionFactory();
		redisTemplate.setConnectionFactory(connectionFactory);
		redisTemplate.setDefaultSerializer(defaultSerializer);
		redisTemplate.afterPropertiesSet();
		return redisTemplate;
	}
	
	/**
	 * 获取Redis连接工厂
	 * @return Redis连接工厂
	 */
	private RedisConnectionFactory getRedisConnectionFactory() {
		JedisConnectionFactory connectionFactory=null;
		if(RedisArch.single==targetArch) {
			RedisStandaloneConfiguration connectConfig = new RedisStandaloneConfiguration();
			connectionFactory = new JedisConnectionFactory(connectConfig);
			if(null!=passWord) connectConfig.setPassword(passWord);
			connectConfig.setHostName(hostList.get(0).host);
			connectConfig.setPort(hostList.get(0).port);
			connectConfig.setDatabase(dbIndex);
		}else if(RedisArch.cluster==targetArch){
			RedisClusterConfiguration connectConfig = new RedisClusterConfiguration();
			 for(Host host:hostList) connectConfig.clusterNode(host.host,host.port);
			connectionFactory=new JedisConnectionFactory(connectConfig);
			 if(null!=passWord) connectConfig.setPassword(passWord);
			 connectConfig.setMaxRedirects(maxRedirect);
		}else{
			connectionFactory=null;
		}
		
		if(null!=connectionFactory) connectionFactory.afterPropertiesSet();
		return connectionFactory;
	}
	
	/**
	 * 初始化主机地址列表
	 */
	private void initHostAddress(){
		String hostListStr=config.getProperty("hostList","").trim();
		String[] hosts=hostListStr.isEmpty()?new String[]{"127.0.0.1:6379"}:COMMA_REGEX.split(hostListStr);
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(host.isEmpty()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2) {
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				hostList.add(new Host(ip,Integer.parseInt(port)));
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				hostList.add(new Host("127.0.0.1",Integer.parseInt(unknow)));
			}else if(IP_REGEX.matcher(unknow).matches()){
				hostList.add(new Host(unknow,6379));
			}
		}
	}
	
	/**
	 * 获取转存日志文件最大尺寸(默认为2GB)
	 */
	private Long getTransferSaveMaxSize(){
		String configMaxVal=config.getProperty("transferSaveMaxSize","").trim();
		if(configMaxVal.isEmpty()) return 2*1024*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 2*1024*1024*1024L;
		return SizeUnit.getBytes(Long.parseLong(matcher.group(1)), matcher.group(2).substring(0,1));
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
			Field field=RedisConfig.class.getDeclaredField(attrName);
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
			Field field=RedisConfig.class.getDeclaredField(attrName);
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
		map.put("hostList", hostList);
		map.put("dbIndex", dbIndex);
		map.put("passWord", passWord);
		map.put("targetArch", targetArch);
		map.put("pluginPath", pluginPath);
		map.put("transferPath", transferPath);
		map.put("maxRedirect", maxRedirect);
		map.put("transferSaveFile", transferSaveFile);
		map.put("targetPipes", Arrays.toString(targetPipes));
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		return map.toString();
	}
}
