package com.df.plugin.sink.hdfs;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.hdfs.config.HdfsConfig;
import com.df.plugin.sink.hdfs.service.HdfsService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description Hdfs发送器
 */
public class HdfsSink extends SinkPluginAdapter{
	/**
	 * Hdfs写出配置
	 */
	private HdfsConfig hdfsConfig;
	
	/**
	 * Hdfs服务配置
	 */
	private HdfsService hdfsService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HdfsSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("HdfsSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.hdfsConfig=new HdfsConfig(flow).config();
		this.hdfsService=new HdfsService(hdfsConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("HdfsSink plugin handing...");
		if(flow.sinkStart) {
			log.info("HdfsSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		
		try{
			while(flow.sinkStart) {
				if(!hdfsService.writeMessage(filterToSinkChannel.get(15000L))) return false;
			}
		}catch(Exception e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		hdfsService.stop();
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("HdfsSink plugin config...");
		if(null==params || 0==params.length) return hdfsConfig.collectRealtimeParams();
		if(params.length<2) return hdfsConfig.getFieldValue((String)params[0]);
		return hdfsConfig.setFieldValue((String)params[0],params[1]);
	}
}
