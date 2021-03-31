package com.df.plugin.sink.flow.dto;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.flow.config.FlowConfig;
import com.df.plugin.sink.flow.consts.TargetType;
import com.github.lixiang2114.flow.comps.Flow;

/**
 * @author Lixiang
 * @description 流程映射器
 */
public class FlowMapper {
	/**
	 * 转存目标文件
	 */
	private File targetFile;
	
	/**
	 * 目标流程对象
	 */
	private Flow targetFlow;
	
	/**
	 * 目标介质类型
	 */
	private TargetType targetType;
	
	/**
	 * 流程发送器配置
	 */
	private FlowConfig flowConfig;
	
	/**
	 * 缓冲流写出器
	 */
	private BufferedWriter bufferedWriter;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FlowMapper.class);
	
	/**
	 * 使用磁盘文件初始化(仅磁盘文件)
	 * @param diskFile 磁盘文件
	 */
	public FlowMapper(File diskFile,FlowConfig flowConfig) {
		this.targetType=TargetType.file;
		this.flowConfig=flowConfig;
		initBufferedWriter(diskFile);
	}
	
	/**
	 * 使用目标流程初始化(带有缓冲和通道)
	 * @param targetFlow 目标流程
	 * @param sendMode 转发模式
	 */
	public FlowMapper(Flow targetFlow,TargetType targetType,FlowConfig flowConfig) {
		this.targetType=targetType;
		this.flowConfig=flowConfig;
		initBufferedWriter(new File((this.targetFlow=targetFlow).sharePath,"buffer.log.0"));
	}
	
	/**
	 * 初始化缓冲写出器
	 * @param targetFile 磁盘文件或缓冲文件
	 */
	private void initBufferedWriter(File targetFile) {
		try{
			bufferedWriter=Files.newBufferedWriter((this.targetFile=targetFile).toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		}catch(Exception e) {
			log.error("create target file stream occur error...",e);
			throw new RuntimeException("create target file stream occur error...");
		}
	}
	
	/**
	 * 写出数据记录
	 * @param line 数据记录行
	 * @throws IOException
	 */
	public void writeMessage(String line) throws Exception {
		if(TargetType.file==targetType.getSuperType()) {
			writeFileLine(line);
		}else{
			writeChannel(line);
		}
	}
	
	/**
	 * 写出文件数据记录
	 * @param line 数据记录行
	 * @throws IOException
	 */
	private void writeFileLine(String line) throws IOException {
		bufferedWriter.write(line);
		bufferedWriter.newLine();
		bufferedWriter.flush();
		
		//当前转存日志文件未达到最大值则继续写转存日志文件
		if(flowConfig.targetFileMaxSize>targetFile.length()) return;
		
		//当前转存日志文件达到最大值则增加转存日志文件
		String curTransSaveFilePath=targetFile.getAbsolutePath();
		int lastIndex=curTransSaveFilePath.lastIndexOf(".");
		if(-1==lastIndex) {
			lastIndex=curTransSaveFilePath.length();
			curTransSaveFilePath=curTransSaveFilePath+".0";
		}
		
		try {
			if(null!=bufferedWriter) bufferedWriter.close();
		} catch (Exception e) {
			log.error("close target file stream occur error...",e);
			throw new RuntimeException("close target file stream occur error...");
		}
		
		File newTargetFile=new File(curTransSaveFilePath.substring(0,lastIndex+1)+(Integer.parseInt(curTransSaveFilePath.substring(lastIndex+1))+1));
		try{
			this.bufferedWriter=Files.newBufferedWriter((this.targetFile=newTargetFile).toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,StandardOpenOption.APPEND);
		}catch(Exception e) {
			log.error("create target file stream occur error...",e);
			throw new RuntimeException("create target file stream occur error...");
		}
		
		log.info("FlowSink switch flow: {} target file to: {}",getFlowName(),newTargetFile.getAbsolutePath());
	}
	
	/**
	 * 写出通道数据记录
	 * @param line 数据记录行
	 * @throws InterruptedException 
	 */
	private void writeChannel(String line) throws InterruptedException {
		if(null==targetFlow) {
			log.error("targetFlow is NULL...");
			throw new RuntimeException("targetFlow is NULL...");
		}
		targetFlow.transferToSourceChannel.put(line);
	}
	
	/**
	 * 获取当前流程名称
	 * @return 流程名称
	 */
	public String getFlowName() {
		return null==targetFlow?null:targetFlow.compName;
	}
	
	/**
	 * 关闭缓冲文件流
	 */
	public void closeFileStream() {
		try {
			if(null!=bufferedWriter) bufferedWriter.close();
		} catch (Exception e) {
			log.error("close target file stream occur error...",e);
			throw new RuntimeException("close target file stream occur error...");
		}
	}

	@Override
	public String toString() {
		return new StringBuilder("flowName:")
				.append(getFlowName()).append(",targetFile:")
				.append(targetFile.getAbsolutePath())
				.toString();
	}

	@Override
	public int hashCode() {
		return targetFile.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(null==obj || !(obj instanceof FlowMapper)) return false;
		return targetFile.equals(((FlowMapper)obj).targetFile);
	}
}
