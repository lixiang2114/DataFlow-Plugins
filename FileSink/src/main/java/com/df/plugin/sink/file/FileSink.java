package com.df.plugin.sink.file;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.file.config.FileConfig;
import com.df.plugin.sink.file.service.FileService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description File发送器
 */
public class FileSink extends SinkPluginAdapter{
	/**
	 * 文件写出配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * 文件服务配置
	 */
	private FileService fileService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("FileSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.fileConfig=new FileConfig(flow).config();
		this.fileService=new FileService(fileConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("FileSink plugin handing...");
		if(flow.sinkStart) {
			log.info("FileSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		
		try{
			String message=null;
			while(flow.sinkStart) {
				if(null==(message=filterToSinkChannel.get())) continue;
				if((message=message.trim()).isEmpty())  continue;
				if(!fileService.writeMessage(message)) return false;
			}
		}catch(Exception e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		fileService.stop();
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("FileSink plugin config...");
		if(null==params || 0==params.length) return fileConfig.collectRealtimeParams();
		if(params.length<2) return fileConfig.getFieldValue((String)params[0]);
		return fileConfig.setFieldValue((String)params[0],params[1]);
	}
}
