package com.df.plugin.source.sql;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.sql.config.SqlConfig;
import com.df.plugin.source.sql.service.SqlService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.ManualPluginAdapter;

/**
 * @author Lixiang
 * @description SQL数据库Source插件
 * 本插件不会删除原始数据表中的记录
 */
public class SqlSource extends ManualPluginAdapter {
	/**
	 * SqlSource配置
	 */
	private SqlConfig sqlConfig;
	
	/**
	 * SqlService服务模块
	 */
	private SqlService sqlService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlSource.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("SqlSource plugin starting...");
		File confFile=new File(pluginPath,"source.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.sqlConfig=new SqlConfig(flow).config();
		this.sqlService=new SqlService(sqlConfig);
		
		return true;
	}
	
	@Override
	public Object handle(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("SqlSource plugin handing...");
		if(flow.sourceStart) {
			log.info("SqlSource is already started...");
			return true;
		}
		
		flow.sourceStart=true;
		return sqlService.startManualETLProcess(sourceToFilterChannel);
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sourceStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("SqlSource plugin config...");
		if(null==params || 0==params.length) return sqlConfig.collectRealtimeParams();
		if(params.length<2) return sqlConfig.getFieldValue((String)params[0]);
		return sqlConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新数据记录检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.info("SqlSource plugin reflesh checkpoint...");
		sqlConfig.refreshCheckPoint();
		return true;
	}
}
