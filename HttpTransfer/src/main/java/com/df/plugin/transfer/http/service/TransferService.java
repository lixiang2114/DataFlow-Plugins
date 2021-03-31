package com.df.plugin.transfer.http.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.http.config.HttpConfig;
import com.df.plugin.transfer.http.config.RecvType;
import com.github.lixiang2114.flow.util.CommonUtil;
import com.github.lixiang2114.netty.handlers.PrintWriter;
import com.github.lixiang2114.netty.scope.HttpServletRequest;
import com.github.lixiang2114.netty.scope.HttpServletResponse;
import com.github.lixiang2114.netty.servlet.HttpAction;

/**
 * @author Lixiang
 * @description 转存服务模块
 */
public class TransferService extends HttpAction {
	/**
	 * Http转存配置
	 */
	private HttpConfig httpConfig;
	
	/**
	 * 文件写出器
	 */
	private BufferedWriter fileWriter;
	
	/**
	 * 认证服务组件
	 */
	private AuthorService authorService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(TransferService.class);
	
	@Override
	public void init() throws IOException {
		this.httpConfig=(HttpConfig)serverConfig.servletConfig;
		this.authorService = new AuthorService(httpConfig);
		this.fileWriter=Files.newBufferedWriter(httpConfig.transferSaveFile.toPath(),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
	}

	/**
	 * 为请求的Http客户端响应ACK确认信息
	 * @param response 响应对象
	 * @throws Exception
	 */
	private static final void writeACK(HttpServletResponse response,String ackMsg) throws Exception {
		PrintWriter writer=response.getPrintWriter();
		try{
			writer.write(ackMsg);
		}finally{
			writer.close();
		}
	}
	
	/**
	 * 客户端登录逻辑
	 * @param request 请求对象
	 * @param response 响应对象
	 * @throws Exception
	 */
	public boolean loginCheck(HttpServletRequest request, HttpServletResponse response) throws Exception {
		if(!httpConfig.requireLogin) return true;
		if(null!=request.getSession().getAttribute("loginUser")) return true;
		
		log.info("current client is not logged, verify the login...");
		
		Boolean flag=null;
		switch(httpConfig.authorMode){
			case "auto":
				flag=authorService.queryAuthor(request, response);
				if(null!=flag && flag) break;
			case "base":
				flag=authorService.baseAuthor(request, response);
				break;
			default:
				flag=authorService.queryAuthor(request, response);
		}
		
		String validateResult=httpConfig.loginFailureId;
		if(null==flag) {
			log.info("parameter config is error,Client Login Failure!");
		}else if(!flag) {
			log.info("userName or passWord is Error,Client Login Failure!");
		}else {
			log.info("Client Login Success!");
			validateResult=httpConfig.loginSuccessId;
			request.getSession().setAttribute("loginUser", request.getAttribute("loginUser"));
		}
		
		writeACK(response,validateResult);
		return false;
	}
	
	@Override
	public void execute(HttpServletRequest request, HttpServletResponse response) throws Exception {
		//如果登录检查不通过则放弃数据
		if(!loginCheck(request,response)) return;
		
		//响应确认数据
		String returnData=httpConfig.normalReply;
		try {
			//获取服务接收到的数据
			String line=null;
			RecvType recvType=httpConfig.recvType;
			switch(recvType){
				case StreamBody:
					 line=request.getStreamBody();
					 break;
				case MessageBody:
					 line=request.getJsonBody();
					 break;
				case QueryString:
					 line=request.getQueryString();
					 break;
				case ParamMap:
					 line=CommonUtil.javaToJsonStr(request.getParametersMap());
					 break;
				default:
					log.info("Error: unknow recvType!");
					return;
			}
			
			if(null==line) return;
			
			//写入转存日志文件
			fileWriter.write(line);
			fileWriter.newLine();
			fileWriter.flush();
			
			//当前转存日志文件未达到最大值则继续写转存日志文件
			if(httpConfig.transferSaveFile.length()<httpConfig.transferSaveMaxSize) return;
			
			//当前转存日志文件达到最大值则增加转存日志文件
			String curTransSaveFilePath=httpConfig.transferSaveFile.getAbsolutePath();
			int lastIndex=curTransSaveFilePath.lastIndexOf(".");
			if(-1==lastIndex) {
				lastIndex=curTransSaveFilePath.length();
				curTransSaveFilePath=curTransSaveFilePath+".0";
			}
			
			httpConfig.transferSaveFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
			fileWriter.close();
			
			log.info("HttpTransfer switch transfer save log file to: "+httpConfig.transferSaveFile.getAbsolutePath());
			fileWriter=Files.newBufferedWriter(httpConfig.transferSaveFile.toPath(),StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		} catch (Exception e) {
			returnData=httpConfig.errorReply;
			log.error("transfer save process running error...",e);
		}finally{
			writeACK(response,returnData);
		}
	}
}
