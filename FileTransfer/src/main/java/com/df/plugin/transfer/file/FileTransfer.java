package com.df.plugin.transfer.file;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.transfer.file.config.FileConfig;
import com.df.plugin.transfer.file.service.FileService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.TransferPluginAdapter;

/**
 * @author Lixiang
 * @description 基于文件系统的Transfer插件
 */
public class FileTransfer extends TransferPluginAdapter {
	/**
	 * FileSource配置
	 */
	private FileConfig fileConfig;
	
	/**
	 * FileService服务组件
	 */
	private FileService fileService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(FileTransfer.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("FileTransfer plugin starting...");
		File confFile=new File(pluginPath,"transfer.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.fileConfig=new FileConfig(flow).config();
		fileService=new FileService(fileConfig);
		
		return true;
	}
	
	@Override
	public Object transfer(Channel<String> transferToSourceChannel) throws Exception {
		log.info("FileTransfer plugin starting...");
		if(flow.transferStart) {
			log.info("FileTransfer is already started...");
			return true;
		}
		
		flow.transferStart=true;
		return fileService.startLogTransferSave(transferToSourceChannel);
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		return fileService.stopLogTransferSave(params);
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("FileTransfer plugin config...");
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
		log.info("FileTransfer plugin reflesh checkpoint...");
		fileConfig.refreshCheckPoint();
		return true;
	}
}
