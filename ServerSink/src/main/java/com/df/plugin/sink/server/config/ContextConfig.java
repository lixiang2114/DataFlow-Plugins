package com.df.plugin.sink.server.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Flow;
import com.github.lixiang2114.flow.context.SizeUnit;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.flow.util.PropertiesReader;

/**
 * @author Lixiang
 * @description 插件上下文配置
 */
@SuppressWarnings("unchecked")
public class ContextConfig {
	/**
	 * 服务端口
	 */
	public int port;
	
	/**
	 * 流程对象
	 */
	public Flow flow;
	
	/**
	 * 发送器运行时路径
	 */
	private File sinkPath;
	
	/**
	 * HTTP登录用户字段名
	 */
	public String userField;
	
	/**
	 * HTTP登录密码字段名
	 */
	public String passField;
	
	/**
	 * 服务器登录用户
	 */
	public String userName;
	
	/**
	 * 服务器登录密码
	 */
	public String passWord;
	
	/**
	 * 缓冲文件已经读取的行数
	 */
	public int lineNumber;
	
	/**
	 * 缓冲文件已经读取的字节数量
	 */
	public long byteNumber;
	
	/**
	 * 推送服务出口行分隔符
	 */
	public String lineSeparator;
	
	/**
	 * 插件配置
	 */
	private Properties config;
	
	/**
	 * 错误回应
	 */
	public String errorReply;
	
	/**
	 * 正常回应
	 */
	public String normalReply;
	
	/**
	 * HTTP认证模式
	 * query:查询字串模式
	 * base:基础认证模式
	 * auto:先使用查询字串,再使用基础认证模式
	 */
	public String authorMode;
	
	/**
	 * 转存文件
	 */
	public File transferSaveFile;
	
	/**
	 * 登录失败标识
	 */
	public String loginFailureId;
	
	/**
	 * 登录成功标识
	 */
	public String loginSuccessId;
	
	/**
	 * 服务是否需要登录
	 */
	public boolean requireLogin;
	
	/**
	 * 是否需要保持HTTP会话
	 */
	public boolean keepSession;
	
	/**
	 * 缓冲日志文件被读完后自动删除
	 */
	public Boolean delOnReaded;
	
	/**
	 * HTTP协议批量发送尺寸
	 */
	public int httpBatchSendSize;
	
	/**
	 * 发送服务协议(TCP/HTTP)
	 */
	public ServerProtocol protocol;
	
	/**
	 * 批量等待最大时间(毫秒)
	 */
	public Long maxBatchWaitMills;
	
	/**
	 * HTTP内容类型
	 */
	public HttpContentType recvType;
	
	/**
	 * 转存文件最大尺寸
	 */
	public Long transferSaveMaxSize;
	
	/**
	 * 默认转存文件
	 */
	public File defaultTransferSaveFile;
	
	/**
	 * 是否在登录成功之后立即推送数据
	 * true:登录成功之后立即推送数据
	 * false:登录成功之后再次请求才推送数据
	 */
	public boolean pushOnLoginSuccess;
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ContextConfig.class);
	
	/**
	 * 容量正则式
	 */
	private static final Pattern CAP_REGEX = Pattern.compile("([1-9]{1}\\d+)([a-zA-Z]{1,5})");
	
	public ContextConfig(){}
	
	public ContextConfig(Flow flow) {
		this.flow=flow;
		this.sinkPath=flow.sinkPath;
		this.defaultTransferSaveFile=new File(sinkPath,"tmp/buffer.log.0");
		this.config=PropertiesReader.getProperties(new File(sinkPath,"sink.properties"));
	}
	
	/**
	 * 服务存储发送器
	 * @param config
	 */
	public ContextConfig config() throws IOException{
		String protocolStr=config.getProperty("protocol","").trim();
		this.protocol=protocolStr.isEmpty()?ServerProtocol.TCP:ServerProtocol.getProtocol(protocolStr);
		
		String requireLoginStr=config.getProperty("requireLogin","").trim();
		this.requireLogin=requireLoginStr.isEmpty()?false:Boolean.parseBoolean(requireLoginStr);
		
		if(requireLogin) {
			String passWordStr=config.getProperty("passWord","").trim();
			this.passWord=passWordStr.isEmpty()?null:passWordStr;
			
			String userNameStr=config.getProperty("userName","").trim();
			this.userName=userNameStr.isEmpty()?null:userNameStr;
			
			if(null==userName || null==passWord) {
				log.error("userName and passWord must be exists when requireLogin is true...");
				throw new RuntimeException("userName and passWord must be exists when requireLogin is true...");
			}
			
			String loginSuccessIdStr=config.getProperty("loginSuccessId","").trim();
			this.loginSuccessId=loginSuccessIdStr.isEmpty()?"OK":loginSuccessIdStr;
			
			String loginFailureIdStr=config.getProperty("loginFailureId","").trim();
			this.loginFailureId=loginFailureIdStr.isEmpty()?"NO":loginFailureIdStr;
			
			if(ServerProtocol.HTTP==protocol) {
				String userFieldStr=config.getProperty("userField","").trim();
				this.userField=userFieldStr.isEmpty()?null:userFieldStr;
				
				String passFieldStr=config.getProperty("passField","").trim();
				this.passField=passFieldStr.isEmpty()?null:passFieldStr;
				
				String authorModeStr=config.getProperty("authorMode","").trim();
				this.authorMode=authorModeStr.isEmpty()?"auto":authorModeStr;
				
				if("auto".equals(authorMode) || "query".equals(authorMode)) {
					if(null==userField || null==passField) {
						log.error("userField and passField must be exists when authorMode is auto or query...");
						throw new RuntimeException("userField and passField must be exists when authorMode is auto or query...");
					}
				}
			}
		}
		
		String pushOnLoginSuccessStr=config.getProperty("pushOnLoginSuccess","").trim();
		this.pushOnLoginSuccess=pushOnLoginSuccessStr.isEmpty()?true:Boolean.parseBoolean(pushOnLoginSuccessStr);
		
		String recvTypeStr=config.getProperty("recvType","").trim();
		this.recvType=recvTypeStr.isEmpty()?HttpContentType.MessageBody:HttpContentType.valueOf(recvTypeStr);
		
		String httpBatchSendSizeStr=config.getProperty("httpBatchSendSize","").trim();
		this.httpBatchSendSize=httpBatchSendSizeStr.isEmpty()?100:Integer.parseInt(httpBatchSendSizeStr);
		
		String keepSessionStr=config.getProperty("keepSession","").trim();
		this.keepSession=keepSessionStr.isEmpty()?false:Boolean.parseBoolean(keepSessionStr);
		
		String lineSeparatorStr=config.getProperty("lineSeparator","").trim();
		this.lineSeparator=lineSeparatorStr.isEmpty()?"\n":lineSeparatorStr;
		
		String normalReplyStr=config.getProperty("normalReply","").trim();
		this.normalReply=normalReplyStr.isEmpty()?"OK":normalReplyStr;
		
		String errorReplyStr=config.getProperty("errorReply","").trim();
		this.errorReply=errorReplyStr.isEmpty()?"NO":errorReplyStr;
		
		String portStr=config.getProperty("port","").trim();
		this.port=portStr.isEmpty()?1567:Integer.parseInt(portStr);
		
		String maxBatchWaitMillStr=config.getProperty("maxBatchWaitMills","").trim();
		this.maxBatchWaitMills=maxBatchWaitMillStr.isEmpty()?15000L:Long.parseLong(maxBatchWaitMillStr);
		
		String transferSaveFileName=config.getProperty("transferSaveFile","").trim();
		this.transferSaveFile=transferSaveFileName.isEmpty()?defaultTransferSaveFile:new File(transferSaveFileName);
		
		File transferDir=transferSaveFile.getParentFile();
		if(!transferDir.exists()) transferDir.mkdirs();
		log.info("transfer save file is: "+transferSaveFile.getAbsolutePath());
		
		transferSaveMaxSize=getTransferSaveMaxSize();
		log.info("transfer save file max size is: "+transferSaveMaxSize);
		
		String delOnReadedStr=config.getProperty("delOnReaded","").trim();
		this.delOnReaded=delOnReadedStr.isEmpty()?true:Boolean.parseBoolean(delOnReadedStr);
		
		String lineNumberStr=config.getProperty("lineNumber","").trim();
		this.lineNumber=lineNumberStr.isEmpty()?0:Integer.parseInt(lineNumberStr);
		
		String byteNumberStr=config.getProperty("byteNumber","").trim();
		this.byteNumber=byteNumberStr.isEmpty()?0:Integer.parseInt(byteNumberStr);
		
		log.info("lineNumber is: "+lineNumber+",byteNumber is: "+byteNumber);
		
		return this;
	}
	
	/**
	 * 获取转存文件最大尺寸(默认为2GB)
	 */
	private Long getTransferSaveMaxSize(){
		String configMaxVal=config.getProperty("transferSaveMaxSize","").trim();
		if(configMaxVal.isEmpty()) return 2*1024*1024*1024L;
		Matcher matcher=CAP_REGEX.matcher(configMaxVal);
		if(!matcher.find()) return 2*1024*1024*1024L;
		return SizeUnit.getBytes(Long.parseLong(matcher.group(1)), matcher.group(2).substring(0,1));
	}
	
	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	public void refreshCheckPoint() throws IOException{
		config.setProperty("lineNumber",""+lineNumber);
		config.setProperty("byteNumber",""+byteNumber);
		config.setProperty("transferSaveFile", transferSaveFile.getAbsolutePath());
		log.info("reflesh checkpoint...");
		OutputStream fos=null;
		try{
			fos=new FileOutputStream(new File(sinkPath,"sink.properties"));
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
			Field field=ContextConfig.class.getDeclaredField(attrName);
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
			Field field=ContextConfig.class.getDeclaredField(attrName);
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
		map.put("sinkPath", sinkPath);
		map.put("transferSaveFile", transferSaveFile);
		map.put("maxBatchWaitMills", maxBatchWaitMills);
		map.put("transferSaveMaxSize", transferSaveMaxSize);
		map.put("defaultTransferSaveFile", defaultTransferSaveFile);
		return map.toString();
	}
}
