package com.df.plugin.source.rt.file;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.rt.file.config.FileConfig;
import com.df.plugin.source.rt.file.config.ScanMode;
import com.df.plugin.source.rt.file.service.FileService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.RealtimePluginAdapter;

/**
 * @author Lixiang
 * @description 基于文件系统的实时Source插件
 */
public class FileRealtime extends RealtimePluginAdapter {
	/**
	 * FileRealtime配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * FileService服务组件
	 */
	private FileService fileService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileRealtime.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("FileRealtime plugin starting...");
		File confFile=new File(pluginPath,"source.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.fileConfig=new FileConfig(flow).config();
		fileService=new FileService(fileConfig);
		
		return true;
	}

	@Override
	public Object handle(Channel<String> transferToETLChannel, Channel<String> sourceToFilterChannel) throws Exception {
		log.info("FileRealtime plugin starting...");
		if(flow.sourceStart) {
			log.info("FileRealtime is already started...");
			return true;
		}
		
		flow.sourceStart=true;
		if(ScanMode.file==fileConfig.scanMode) {
			return fileService.startRealtimeETLProcess(sourceToFilterChannel);
		}
		
		String message = null;
		while(flow.sourceStart){
			if (null==(message=(String)transferToETLChannel.get())) continue;
			if((message=message.trim()).isEmpty()) continue;
			sourceToFilterChannel.put(message);
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sourceStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("FileRealtime plugin config...");
		if(null==params || 0==params.length) return fileConfig.collectRealtimeParams();
		if(params.length<2) return fileConfig.getFieldValue((String)params[0]);
		return fileConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.info("FileRealtime plugin reflesh checkpoint...");
		fileConfig.refreshCheckPoint();
		return true;
	}
}
