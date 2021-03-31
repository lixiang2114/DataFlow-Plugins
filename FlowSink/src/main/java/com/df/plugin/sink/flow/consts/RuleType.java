package com.df.plugin.sink.flow.consts;

/**
 * @author Lixiang
 * @description 规则类型
 */
public enum RuleType {
	/**
	 * 字典键字类型
	 */
	key("key"),

	/**
	 * 序列索引类型
	 */
	index("index");
	
	/**
	 * 规则类型名称
	 */
	public String typeName;
	
	private RuleType(String typeName) {
		this.typeName=typeName;
	}
}
