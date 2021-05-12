package com.df.plugin.sink.server.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.server.config.ContextConfig;
import com.github.lixiang2114.netty.event.AbstractTcpEventAdapter;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author Lixiang
 * @description TCP发送服务模块
 */
public class TcpService extends AbstractTcpEventAdapter {
	/**
	 * 上下文配置
	 */
	private ContextConfig config;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(TcpService.class);
	
	/**
	 * 本地线程字典
	 */
	private static final ThreadLocal<Boolean> threadLocal=new ThreadLocal<Boolean>();
	
	/**
	 * 会话作用域字典
	 */
	private static final ConcurrentHashMap<String, Object> session=new ConcurrentHashMap<String, Object>();
	
	@Override
	public void init() throws Exception {
		this.config=(ContextConfig)serverConfig.appConfig;
	}

	@Override
	public void onActived(ChannelHandlerContext context) throws Exception {
		if(!loginCheck(null)) return;
		log.info("sync execute server sink process...");
		threadLocal.set(true);
		pushData(context);
	}
	
	@Override
	public void onMessaged(ChannelHandlerContext context, String message) throws Exception {
		Boolean isLogin=threadLocal.get();
		if(null!=isLogin && isLogin) return;
		if(!loginCheck(message)) return;
		if(config.pushOnLoginSuccess) pushData(context);
	}

	@Override
	public void onExceptioned(ChannelHandlerContext context, Throwable cause) {
		log.error("Occur Error:"+cause.getMessage(),cause);
		context.close();
	}
	
	/**
	 * 推送数据到Socket通道
	 * @param context 通道上下文
	 * @throws Exception
	 */
	private void pushData(ChannelHandlerContext context) throws Exception {
		RandomAccessFile raf=null;
		Channel channel=context.channel();
		try{
			out:while(config.flow.sinkStart) {
				raf=new RandomAccessFile(config.transferSaveFile, "r");
				if(0!=config.byteNumber)raf.seek(config.byteNumber);
				while(config.flow.sinkStart) {
					if(!channel.isActive()) {
						log.warn("tcp client:{} close connect...",channel.remoteAddress());
						break out;
					}
					
					String line=raf.readLine();
					if(null==line) {
						if(!isLastFile()){
							nextFile(raf);
							break;
						}
						
						try{
							Thread.sleep(3000L);
						}catch(InterruptedException e){
							log.warn("ServerSink sleep interrupted,realtime process is over...");
							break out;
						}
						
						continue;
					}
					
					channel.writeAndFlush(line.trim()+config.lineSeparator);
					config.lineNumber=config.lineNumber++;
					config.byteNumber=raf.getFilePointer();
				}
			}
		
			log.info("ServerSink plugin realtime process normal exit,execute checkpoint...");
		}catch(Exception e){
			log.error("ServerSink plugin realtime process running error...",e);
		}finally{
			try {
				config.refreshCheckPoint();
			} catch (IOException e) {
				log.error("ServerSink plugin realtime refreshCheckPoint occur Error...",e);
			}
			try{
				if(null!=raf) raf.close();
				if(null!=channel) channel.close();
			}catch(IOException e){
				log.error("ServerSink plugin close random file stream occur Error...",e);
			}
		}
	}

	/**
	 * 登录验证
	 * @return 是否登录成功
	 * @throws Exception
	 */
	private boolean loginCheck(String message) throws Exception {
		if(!config.requireLogin) return true;
		if(null!=session.get("loginUser")) return true;
		if(null==message) return false;
		if((message=message.trim()).isEmpty()) return false;
		
		String[] userAndPass=Author.parseBaseAuthorInfo(message);
		if(null==userAndPass || 2>userAndPass.length) return false;
		
		String userName=userAndPass[0];
		String passWord=userAndPass[1];
		if(config.userName.equalsIgnoreCase(userName) && config.passWord.equalsIgnoreCase(passWord)) {
			session.put("loginUser", Collections.singletonMap(userName, passWord));
			return true;
		}else{
			return false;
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
		log.info("ServerSink switch buffer file to "+config.transferSaveFile.getAbsolutePath());
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
}
