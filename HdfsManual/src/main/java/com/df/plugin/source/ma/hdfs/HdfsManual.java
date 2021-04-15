package com.df.plugin.source.ma.hdfs;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.ma.hdfs.config.HdfsConfig;
import com.df.plugin.source.ma.hdfs.service.HdfsService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.ManualPluginAdapter;

/**
 * @author Lixiang
 * @description 基于HDFS分布式存储的离线Source插件
 */
public class HdfsManual extends ManualPluginAdapter {
	/**
	 * HdfsManual配置
	 */
	private HdfsConfig hdfsConfig;
	
	/**
	 * HdfsService服务组件
	 */
	private HdfsService hdfsService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HdfsManual.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("HdfsManual plugin starting...");
		File confFile=new File(pluginPath,"source.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.hdfsConfig=new HdfsConfig(flow).config();
		hdfsService=new HdfsService(hdfsConfig);
		
		return true;
	}
	
	@Override
	public Object handle(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("HdfsManual plugin handing...");
		if(flow.sourceStart) {
			log.info("HdfsManual is already started...");
			return true;
		}
		
		flow.sourceStart=true;
		return hdfsService.startManualETLProcess(sourceToFilterChannel);
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sourceStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("HdfsManual plugin config...");
		if(null==params || 0==params.length) return hdfsConfig.collectRealtimeParams();
		if(params.length<2) return hdfsConfig.getFieldValue((String)params[0]);
		return hdfsConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.info("HdfsManual plugin reflesh checkpoint...");
		hdfsConfig.refreshCheckPoint();
		return true;
	}
}
