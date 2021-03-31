package com.df.plugin.sink.flow.consts;

/**
 * @author Lixiang
 * @description 发送模式
 */
public enum SendMode {
	/**
	 * 复制到每一个目标
	 */
	rep("rep"),
	
	/**
	 * 负载均衡到目标组
	 */
	lbc("lbc"),
	
	/**
	 * 根据字段规则转发
	 */
	field("field"),
	
	/**
	 * 自定义转发组策略
	 */
	custom("custom"),
	
	/**
	 * 负载均衡-哈希到目标组
	 * 哈希数组求模法
	 */
	hash("hash","lbc"),
	
	/**
	 * 负载均衡-轮训到目标组
	 * 平均轮训分发法
	 */
	robin("robin","lbc"),
	
	/**
	 * 负载均衡-随机到目标组
	 * 任意随机转发法
	 */
	random("random","lbc");
	
	/**
	 * 模式名称
	 */
	public String name;
	
	/**
	 * 模式父级名称
	 */
	public String parentName;
	
	public String getSuperName() {
		return null==parentName?name:parentName;
	}
	
	public SendMode getSuperType() {
		return SendMode.valueOf(getSuperName());
	}
	
	private SendMode(String name) {
		this(name,null);
	}
	
	private SendMode(String name,String parentName) {
		this.name=name;
		this.parentName=parentName;
	}
}
