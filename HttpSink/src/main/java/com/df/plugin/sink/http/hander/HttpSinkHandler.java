package com.df.plugin.sink.http.hander;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;

import com.df.plugin.sink.http.config.HttpConfig;
import com.df.plugin.sink.http.service.AuthorService;
import com.github.lixiang2114.flow.util.RestClient;
import com.github.lixiang2114.flow.util.RestClient.WebResponse;

/**
 * @author Lixiang
 * @description HTTP发送器操作句柄
 */
public class HttpSinkHandler implements Runnable{
	/**
	 * HTTP发送器配置
	 */
	private HttpConfig config;
	
	/**
	 * 消息是否被放置于查询字段上
	 */
	private boolean isMessageField;
	
	/**
	 * HTTP消息头字典
	 */
	private HttpHeaders httpHeader;
	
	/**
	 * HTTP认证服务组件
	 */
	private AuthorService authorService;
	
	/**
	 * HTTP消息体字典
	 */
	private LinkedMultiValueMap<String,Object> httpBody;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HttpSinkHandler.class);
	
	public HttpSinkHandler(HttpConfig config) {
		this.config=config;
		this.authorService=new AuthorService(config);
		this.isMessageField=null!=config.messageField;
		this.httpHeader=RestClient.getDefaultRequestHeader();
		this.httpBody=new LinkedMultiValueMap<String,Object>();
		this.initProtocolData();
	}
	
	/**
	 * 初始化HTTP协议数据
	 */
	private void initProtocolData() {
		MediaType mediaType=null;
		switch(config.sendType){
			case MessageBody:
				mediaType=MediaType.APPLICATION_JSON_UTF8;
				break;
			case StreamBody:
				mediaType=MediaType.APPLICATION_OCTET_STREAM;
				break;
			case QueryString:
			case ParamMap:
			default:
				mediaType=MediaType.APPLICATION_FORM_URLENCODED;
		}
		
		httpHeader.setContentType(mediaType);
		if(!config.requireLogin) return;
		
		if("base".equalsIgnoreCase(config.authorMode)) {
			httpHeader.set(HttpHeaders.AUTHORIZATION,authorService.getBaseRealm());
		}else{
			httpBody.add(config.userField, config.userName);
			httpBody.add(config.passField, config.passWord);
		}
	}
	
	@Override
	public void run() {
		if(config.pushOnLoginSuccess) {
			pushDataLine(); //登录同时推送数据(每次请求都是终端登录+数据提交)
		}else{
			if(!login()) { //先请求登录
				log.error("login http server failure...");
				return;
			}
			pushDataLine(); //登录成功后再持续请求推送数据
		}
	}
	
	/**
	 * 推送数据行
	 */
	private void pushDataLine() {
		log.info("sync execute loginAndPush process...");
		RandomAccessFile raf=null;
		try{
			out:while(config.flow.sinkStart) {
				raf=new RandomAccessFile(config.transferSaveFile, "r");
				if(0!=config.byteNumber)raf.seek(config.byteNumber);
				while(config.flow.sinkStart) {
					String line=raf.readLine();
					if(null==line) {
						if(!isLastFile()) {
							nextFile(raf);
							break;
						}
						
						try{
							Thread.sleep(3000L);
						}catch(InterruptedException e){
							log.warn("sink sleep interrupted,ETL is over...");
							break out;
						}
						
						continue;
					}
					
					if(!push(line.trim())) throw new RuntimeException("push data to http server failure...");
					config.lineNumber=config.lineNumber++;
					config.byteNumber=raf.getFilePointer();
				}
			}
			
			log.info("HttpSink plugin realtime process normal exit,execute checkpoint...");
		}catch(Exception e){
			log.error("HttpSink plugin realtime process running error...",e);
		}finally{
			try {
				config.refreshCheckPoint();
			} catch (IOException e) {
				log.error("HttpSink plugin realtime refreshCheckPoint occur Error...",e);
			}
			
			if(null!=raf) {
				try{
					 raf.close();
				}catch(IOException e){
					log.error("HttpSink plugin close random file stream occur Error...",e);
				}
			}
		}
	}
	
	/**
	 * 切换下一个文件
	 * @return 是否是最后一个文件
	 * @throws IOException 
	 */
	private void nextFile(RandomAccessFile randomFile) throws IOException{
		config.byteNumber=0;
		config.lineNumber=0;
		if(null!=randomFile) randomFile.close();
		String curFilePath=config.transferSaveFile.getAbsolutePath();
		
		if(config.delOnReaded)config.transferSaveFile.delete();
		int lastIndex=curFilePath.lastIndexOf(".");
		int newIndex=Integer.parseInt(curFilePath.substring(lastIndex+1))+1;
		config.transferSaveFile=new File(curFilePath.substring(0,lastIndex+1)+newIndex);
		log.info("HttpSink switch buffer file to "+config.transferSaveFile.getAbsolutePath());
	}
	
	/**
	 * 是否读到最后一个文件
	 * @return 是否是最后一个文件
	 */
	private boolean isLastFile(){
		String curFilePath=config.transferSaveFile.getAbsolutePath();
		int curFileIndex=Integer.parseInt(curFilePath.substring(curFilePath.lastIndexOf(".")+1));
		for(String fileName:config.transferSaveFile.getParentFile().list()) if(curFileIndex<Integer.parseInt(fileName.substring(fileName.lastIndexOf(".")+1))) return false;
		return true;
	}
	
	/**
	 * 两段式之终端登录到HTTP服务器
	 * @return 是否登录成功
	 */
	private boolean login(){
		if(!config.requireLogin) return true;
		
		boolean flag=false;
		HashMap<String,Object> respMap=new HashMap<String,Object>();
		
		if("base".equalsIgnoreCase(config.authorMode)) {
			flag=authorService.baseAuthor(respMap,httpHeader);
		}else{
			flag=authorService.queryAuthor(respMap,httpBody);
		}
		
		if(!flag) {
			log.error("HttpSink login failure,response status code: {},response message: {}...",respMap.remove("statusCode"),respMap.remove("respMsg"));
			return false;
		}
		
		httpHeader.remove(HttpHeaders.AUTHORIZATION);
		httpBody.remove(config.userField);
		httpBody.remove(config.passField);
		return true;
	}
	
	/**
	 * 推送数据到HTTP服务器
	 * @param message 消息内容
	 * @return 是否推送成功
	 * @throws Exception
	 */
	private boolean push(String message) throws Exception{
		if(isMessageField) httpBody.add(config.messageField, message);
		long failWaitMills=config.failMaxWaitMills;
		boolean loop=false;
		int times=0;
		do{
			try{
				WebResponse<String> webResponse=null;
				if(httpBody.isEmpty()){
					webResponse=RestClient.post(config.postURL,httpHeader,message,new Object[0]);
				}else{
					webResponse=RestClient.post(config.postURL,httpHeader,httpBody,new Object[0]);
				}
				
				if(null==webResponse) throw new RuntimeException("HttpSink send message failure,webResponse is NULL...");
				if(HttpStatus.OK.value()!=webResponse.getStatusCode()) throw new RuntimeException("HttpSink send message failure,status code is: "+webResponse.getStatusCode());
				
				loop=false;
			}catch(Exception e) {
				times++;
				loop=true;
				Thread.sleep(failWaitMills=failWaitMills+1000);
				log.error("send occur excepton: "+e.getMessage(),e);
			}
		}while(loop && config.flow.sinkStart && times<config.maxRetryTimes);
		
		return !loop;
	}
}
