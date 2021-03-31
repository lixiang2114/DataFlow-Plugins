package com.df.plugin.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.FilterPluginAdapter;

/**
 * @author Lixiang
 * @description 默认过滤器
 */
public class DefaultFilter extends FilterPluginAdapter{
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(DefaultFilter.class);

	@Override
	public Object filter(Channel<String> sourceToFilterChannel,Channel<String> filterToSinkChannel) throws Exception{
		log.info("DefaultFilter plugin filtering...");
		if(flow.filterStart) {
			log.info("DefaultFilter is already started...");
			return true;
		}
		
		flow.filterStart=true;
		try{
			String message=null;
			while(flow.filterStart) {
				if(null==(message=sourceToFilterChannel.get())) continue;
				if((message=message.trim()).isEmpty()) continue;
				filterToSinkChannel.put(message);
			}
		}catch(InterruptedException e){
			log.warn("filter plugin is interrupted while waiting...");
		}
		
		return true;
	}

	@Override
	public Object stop(Object params) throws Exception {
		flow.filterStart=false;
		return true;
	}
}
