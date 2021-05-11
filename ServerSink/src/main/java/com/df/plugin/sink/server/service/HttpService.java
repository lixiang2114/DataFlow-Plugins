package com.df.plugin.sink.server.service;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.df.plugin.sink.server.config.ContextConfig;
import com.github.lixiang2114.netty.handlers.PrintWriter;
import com.github.lixiang2114.netty.scope.HttpServletRequest;
import com.github.lixiang2114.netty.scope.HttpServletResponse;
import com.github.lixiang2114.netty.scope.HttpSession;
import com.github.lixiang2114.netty.servlet.HttpAction;

/**
 * @author Lixiang
 * @description HTTP发送服务模块
 */
public class HttpService extends HttpAction {
	/**
	 * 上下文配置
	 */
	private ContextConfig config;
	
	/**
	 * HTTP认证服务组件
	 */
	private HttpAuthor httpAuthor;
	
	/**
	 * 是否需要持久化检查点
	 */
	private boolean checkpoint=true;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HttpService.class);
	
	@Override
	public void init() throws IOException {
		this.config=(ContextConfig)serverConfig.appConfig;
		this.httpAuthor = new HttpAuthor(config);
	}
	
	/**
	 * 发送数据到Socket通道
	 * @param response 响应对象
	 * @param messages 消息列表
	 * @throws Exception 异常对象
	 */
	private void sendDatas(HttpServletResponse response,List<String> messages) throws Exception {
		PrintWriter writer=response.getPrintWriter();
		try{
			String resp=StringUtils.collectionToDelimitedString(messages, config.lineSeparator);
			this.checkpoint=resp.isEmpty()?false:true;
			writer.write(resp);
		}finally{
			writer.close();
			messages.clear();
		}
	}
	
	/**
	 * 客户端登录逻辑
	 * @param request 请求对象
	 * @param response 响应对象
	 * @throws Exception 异常对象
	 */
	private boolean loginCheck(HttpServletRequest request, HttpServletResponse response) throws Exception {
		if(!config.requireLogin) return true;
		HttpSession session=request.getSession();
		if(null!=session && null!=session.getAttribute("loginUser")) return true;
		
		log.info("current client is not logged, verify the login...");
		
		Boolean flag=null;
		switch(config.authorMode){
			case "auto":
				flag=httpAuthor.queryAuthor(request, response);
				if(null!=flag && flag) break;
			case "base":
				flag=httpAuthor.baseAuthor(request, response);
				break;
			default:
				flag=httpAuthor.queryAuthor(request, response);
		}
		
		String validateResult=config.loginFailureId;
		if(null==flag) {
			log.error("uri:{} parameter config is error,Client Login Failure!",request.getRequestURI());
		}else if(!flag) {
			log.error("uri:{} userName or passWord is Error,Client Login Failure!",request.getRequestURI());
		}else {
			log.info("Client Login Success!");
			validateResult=config.loginSuccessId;
			if(null!=session) session.setAttribute("loginUser", request.getAttribute("loginUser"));
		}
		
		if(config.pushOnLoginSuccess && null!=flag && flag) return true;
		sendDatas(response,Collections.singletonList(validateResult));
		return false;
	}
	
	/**
	 * @description Web客户端获得响应后会立即关闭Socket通道,因此这里写完消息就直接关闭Socket即可
	 */
	@Override
	public void execute(HttpServletRequest request, HttpServletResponse response) throws Exception {
		if(!loginCheck(request,response)) return;
		log.info("sync execute server sink process...");
		
		RandomAccessFile raf=null;
		ArrayList<String> lineList=new ArrayList<String>();
		try{
			out:while(config.flow.sinkStart) {
				raf=new RandomAccessFile(config.transferSaveFile, "r");
				if(0!=config.byteNumber)raf.seek(config.byteNumber);
				while(config.flow.sinkStart) {
					String line=raf.readLine();
					if(null==line) {
						if(isLastFile()) {
							sendDatas(response,lineList);
							break out;
						}
						nextFile(raf);
						break;
					}
					
					config.byteNumber=raf.getFilePointer();
					config.lineNumber=config.lineNumber++;
					
					String record=line.trim();
					if(record.isEmpty()) continue;
					
					lineList.add(record);
					if(config.httpBatchSendSize>lineList.size()) continue;
					sendDatas(response,lineList);
					break out;
				}
			}
			
			log.info("ServerSink plugin realtime process normal exit,execute checkpoint...");
		}catch(Exception e){
			log.error("ServerSink plugin realtime process running error...",e);
		}finally{
			if(checkpoint) {
				try {
					config.refreshCheckPoint();
				} catch (IOException e) {
					log.error("ServerSink plugin realtime refreshCheckPoint occur Error...",e);
				}
			}
			
			if(null!=raf) {
				try{
					 raf.close();
				}catch(IOException e){
					log.error("ServerSink plugin close random file stream occur Error...",e);
				}
			}
		}
	}
	
	/**
	 * 切换下一个日志文件
	 * @return 是否是最后一个日志文件
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
	 * 是否读到最后一个日志文件
	 * @return 是否是最后一个日志文件
	 */
	private boolean isLastFile(){
		String curFilePath=config.transferSaveFile.getAbsolutePath();
		int curFileIndex=Integer.parseInt(curFilePath.substring(curFilePath.lastIndexOf(".")+1));
		for(String fileName:config.transferSaveFile.getParentFile().list()) if(curFileIndex<Integer.parseInt(fileName.substring(fileName.lastIndexOf(".")+1))) return false;
		return true;
	}
}
