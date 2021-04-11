package com.df.plugin.transfer.redis.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.ClusterOperations;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.Expiration;

import com.github.lixiang2114.flow.util.ApplicationUtil;

/**
 * @author Lixiang
 * Redis缓存操作工具集
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class RedisUtil {
	/**
     * 模糊匹配域脚本
     */
    private static final String HKEYS_LUA;
    
	/**
     * 分布式解锁脚本
     */
    private static final String UNLOCK_LUA;
    
	/**
     * 不设置过期时长
     */
    public static final long NOT_EXPIRE = -1;
    
    /**
     * 默认过期时长,单位:毫秒 (10秒)
     */
    public static final long DEFAULT_EXPIRE = 10000L;
    
    /**
     * 同步锁默认超时时间(单位:秒)
     */
    public static final int DEFAULT_LOCK_TIMEOUT = 2;
    
    /**
     * 尝试获取同步锁的间隔时间(单位:毫秒)
     */
    public static final long TRY_LOCK_INTERVAL = 100L;
    
    /**
	 * 默认消息队列名
	 */
	public static final String DEFAULT_QUEUE="Redis-Queue";
	
	/**
	 * 列表操作工具
	 */
	private ListOperations<String, Object> listOperations;
	
	/**
	 * Redis缓存通用操作工具
	 */
	private RedisTemplate<String,Object> redisTemplate;
	
	/**
	 * 集合操作工具
	 */
	private SetOperations<String, Object> setOperations;
	
	/**
	 * 有序集合操作工具
	 */
	private ZSetOperations<String, Object> zsetOperations;
	
	/**
     * 队列最大阻塞超时时间
     */
    public static final long QUEUE_MAX_PULL_TIMEOUT = 5000L;
	
	/**
	 * 字串操作工具
	 */
	private ValueOperations<String, Object> valueOperations;
	
	/**
	 * 集群操作
	 */
	private ClusterOperations<String,Object> clusterOperations;
	
	/**
	 * Hash表操作工具
	 */
	private HashOperations<String, String, Object> hashOperations;
	
	/**
     * 缓存Redis脚本函数
     */
    private static HashMap<String,String> funToScriptDict=new HashMap<String,String>();
	
	/**
     * Lua脚本初始化
     */
    static {
        StringBuilder lockBuilder = new StringBuilder();
        lockBuilder.append("if redis.call('get',KEYS[1]) == ARGV[1] then ");
        lockBuilder.append("return redis.call('del',KEYS[1]);");
        lockBuilder.append("else ");
        lockBuilder.append("return 0;");
        lockBuilder.append("end");
        UNLOCK_LUA = lockBuilder.toString();
        
        StringBuilder hkeysBuilder = new StringBuilder();
        hkeysBuilder.append("local resultArr=redis.call('hkeys',KEYS[1]);");
        hkeysBuilder.append("local resultDict={};");
        hkeysBuilder.append("for index,value in pairs(resultArr) do ");
        hkeysBuilder.append("if(string.find(value,ARGV[1])) then ");
        hkeysBuilder.append("table.insert(resultDict,value);");
        hkeysBuilder.append("end;end;return resultDict");
        HKEYS_LUA=hkeysBuilder.toString();
    }
    
    public RedisUtil() {}
    
    public RedisUtil(RedisTemplate redisTemplate) {
    	setRedisTemplate(redisTemplate);
    }
	
	/**
	 * 获取Spring-RedisTemplate
	 */
	public RedisTemplate getRedisTemplate() {
		return null==redisTemplate?redisTemplate=ApplicationUtil.getBean("redisTemplate",RedisTemplate.class):redisTemplate;
	}
	
	/**
	 * 初始化Spring-RedisTemplate
	 * @param redisTemplate
	 */
	public void setRedisTemplate(RedisTemplate redisTemplate) {
		if(null==redisTemplate) return;
		this.redisTemplate=redisTemplate;
		this.clusterOperations=redisTemplate.opsForCluster();
		this.hashOperations=redisTemplate.opsForHash();
		this.valueOperations=redisTemplate.opsForValue();
		this.listOperations=redisTemplate.opsForList();
		this.setOperations=redisTemplate.opsForSet();
		this.zsetOperations=redisTemplate.opsForZSet();
	}
	
	/**
	 * 查找匹配的键
	 * @param pattern 字串表达式
	 * @return 键集
	 * @description pattern示例如下:
	 * 匹配结尾的前缀:*ini、*.png
	 * 匹配开头的前缀:userScore*、userScore.*
	 */
	public Set<String> keys(String pattern) {
		return redisTemplate.keys(pattern);
	}
	
	/**
	 * 查找Hash表中匹配的域
	 * @param pattern 字串表达式
	 * @return 域集
	 * @description pattern参数解释:
	 * 只要键串中包含参数串pattern就匹配成功
	 */
	public Set<String> hkeys(String key,String pattern) {
		List<byte[]> result=null;
		String functionName=funToScriptDict.get("HKEYS_LUA");
		if(null!=functionName){
			try{
				RedisCallback<List<byte[]>> redisCallback=connection->connection.evalSha(functionName, ReturnType.MULTI, 1, key.getBytes(Charset.forName("UTF-8")),pattern.getBytes(Charset.forName("UTF-8")));
				result=redisTemplate.execute(redisCallback);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		if(null==result || result.isEmpty()) {
			DefaultRedisScript<List<byte[]>> redisScript = new DefaultRedisScript(HKEYS_LUA,List.class);
			funToScriptDict.put("HKEYS_LUA", redisScript.getSha1());
			result=redisTemplate.execute(redisScript, Arrays.asList(key), Arrays.asList(pattern));
		}
		
		if(null==result || result.isEmpty()) return null;
		return result.stream().map(b->new String(b)).collect(Collectors.toSet());
	}
	
	/**
     * 判断指定键是否存在
     * @param key 键
     * @return 是否存在指定的键
     */
    public boolean hasKey(String key) {
        return redisTemplate.hasKey(key);
    }
    
    /**
     * 判断指定域是否存在
     * @param key 键
     * @param field 域
     * @return 是否存在指定的域
     */
    public boolean hasField(String key,String field) {
        return hashOperations.hasKey(key, field);
    }
    
    /**
     * 为指定键的映射设置过期时长
     * @param key 键
     * @param expire 过期时间(单位:天数)
     * @return 是否存在指定的键
     */
    public long expireInDays(String key, Integer expire) {
    	return expire(key, expire.longValue(), TimeUnit.DAYS);
    }
    
    /**
     * 为指定键的映射设置过期时长
     * @param key 键
     * @param expire 过期时间(单位:小时)
     * @return 是否存在指定的键
     */
    public long expireInHours(String key, Long expire) {
    	return expire(key, expire, TimeUnit.HOURS);
    }
    
    /**
     * 为指定键的映射设置过期时长
     * @param key 键
     * @param expire 过期时间(单位:分钟)
     * @return 是否存在指定的键
     */
    public long expireInMinus(String key, Long expire) {
    	return expire(key, expire, TimeUnit.MINUTES);
    }
    
    /**
     * 为指定键的映射设置过期时长
     * @param key 键
     * @param expire 过期时间(单位:秒)
     * @return 是否存在指定的键
     */
    public long expireInSecs(String key, Long expire) {
    	return expire(key, expire, TimeUnit.SECONDS);
    }
    
    /**
     * 为指定键的映射设置过期时长
     * @param key 键
     * @param expire 过期时间(单位:毫秒)
     * @return 是否存在指定的键
     */
    public long expireInMills(String key, Long expire) {
    	return expire(key, expire, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 为指定键的映射设置过期时长
     * @param key 键
     * @param expire 过期时间
     * @param unit 过期时间单位
     * @return 是否存在指定的键
     */
    public long expire(String key, Long expire,TimeUnit unit) {
        redisTemplate.expire(key, expire, unit);
        if(redisTemplate.hasKey(key)) return 1L;
        return 0L;
    }
    
    /**
     * 为键的映射值增长一个指定的整数值
     * @param key 键
     * @param increment 增长量(可以为负值)
     * @return Value增长后的值
     */
    public long incrBy(String key, Long increment) {
		return valueOperations.increment(key, increment);
	}
	
	/**
	 * 设置键值对
	 * @param key 键
	 * @param value 值
	 */
	public void set(String key,Object value){
		valueOperations.set(key, value);
	}
	
	/**
	 * 设置键值对
	 * @param key 键
	 * @param value 值
	 * @param expired 过期时间毫秒数
	 */
	public void set(String key,Object value,Long expired){
		valueOperations.set(key, value, Duration.ofMillis(expired));
	}
	
	/**
	 * 根据键获得值
	 * @param key 键
	 * @return 对象
	 */
	public Object get(String key){
		return valueOperations.get(key);
	}
	
	/**
	 * 根据键获得值并更新过期时间
	 * @param key 键
	 * @param expire 过期时间毫秒数
	 * @return 对象
	 */
	public Object get(String key,Integer expire){
		Object value=valueOperations.get(key);
		expireInMills(key,expire.longValue());
		return value;
	}
	
	/**
	 * 根据键获得值
	 * @param key 键
	 * @return 泛化类型
	 */
	public <R> R get(String key,Class<R> type){
		return type.cast(valueOperations.get(key));
	}
	
	/**
	 * 根据键获得值并更新过期时间
	 * @param key 键
	 * @param expire 过期时间毫秒数
	 * @return 泛化类型
	 */
	public <R> R getAndExpire(String key,Class<R> type,Integer expire){
		R r=type.cast(valueOperations.get(key));
		expireInMills(key,expire.longValue());
		return r;
	}
	
	/**
	 * 删除指定键的键值对
	 * @param key 键
	 * @return 是否删除成功
	 */
	public boolean del(String key){
		return redisTemplate.delete(key);
	}
	
	/**
	 * 删除所有模糊匹配到的键
	 * @param pattern 键字串表达式
	 * @return 被删除键的数量
	 */
    public  long delAll(String pattern) {
    	Set<String> keys=redisTemplate.keys(pattern);
    	return redisTemplate.delete(keys);
    }
    
    /**
	 * List集合尺寸(集合中元素数量)
	 * @param key 标识集合的键
	 * @return 集合尺寸
	 */
    public long listSize(String key) {
    	return listOperations.size(key);
    }
    
    /**
	 * 推送指定值序列到给定key标记的列表中
	 * @param key 键
	 * @param values 值序列
	 */
	public  void leftPush(String key,Object... values){
		if(null==values || 0==values.length) return;
		listOperations.leftPushAll(key, values);
	}
    
    /**
     * 修改列表集合中下标处的值
     * @param key 键
     * @param index 下标
     * @param value 值
     */
    public void listSet(String key, Long index,Object value) {
        listOperations.set(key, index, value);
    }
    
    /**
     * 获取列表集合中下标处的值
     * @param key 键
     * @param index 下标
     * @return 下标处的值
     */
    public Object listGet(String key, Long index) {
        return listOperations.index(key, index);
    }
    
    /**
     * 获取列表集合中下标处的值
     * @param key 键
     * @param index 下标
     * @return 下标处的值
     */
    public <R> R listGet(String key, Long index,Class<R> returnType) {
    	Object value=listOperations.index(key, index);
        return returnType.cast(value);
    }
    
    /**
     * 从列表中删除元素值等于value的所有元素
     * @param key 键
     * @param value 需要删除的元素值
     * @return 实际删除的数量
     */
    public Long listDelAll(String key, Object value) {
        return listOperations.remove(key, 0,value);
    }
    
    /**
     * 从列表中删除元素值等于value的第一个(从左到右)元素
     * @param key 键
     * @param value 需要删除的元素值
     * @return 实际删除的数量
     */
    public Long listDelFirst(String key, Object value) {
        return listOperations.remove(key, 1,value);
    }
    
    /**
     * 从列表中删除元素值等于value的最后一个(从右到左)元素
     * @param key 键
     * @param value 需要删除的元素值
     * @return 实际删除的数量
     */
    public Long listDelLast(String key, Object value) {
        return listOperations.remove(key, -1,value);
    }
    
    /**
     * 从列表中删除元素值等于value的count个元素
     * @param key 键
     * @param count 删除数量
     * @param value 需要删除的元素值
     * @return 实际删除的数量
     */
    public Long listDel(String key, Long count,Object value) {
        return listOperations.remove(key, count,value);
    }
	
	/**
	 * 从列表右边弹出元素,若列表中没有任何元素则返回null
	 * 该方法是不会阻塞的
	 * @param key 键
	 * @return 弹出的对象
	 */
	public Object rightPop(String key){
		return listOperations.rightPop(key);
	}
	
	/**
	 * 从列表右边弹出元素,若列表中没有任何元素则返回null
	 * 该方法是不会阻塞的
	 * @param key 键
	 * @param returnType 返回类型
	 * @return 泛化类型
	 */
	public <R> R rightPop(String key,Class<R> returnType){
		Object value=listOperations.rightPop(key);
		return returnType.cast(value);
	}
	
	/**
	 * 阻塞直到列表中有元素后从列表右边弹出元素,阻塞超时后返回null
	 * 该方法是阻塞的
	 * @param key 键
	 * @param maxWaitMills 最大等待时间(单位:毫秒)
	 * @return 弹出的对象
	 */
	public Object rightPop(String key,Long maxWaitMills){
		return listOperations.rightPop(key, maxWaitMills, TimeUnit.MILLISECONDS);
	}
	
	/**
	 * 阻塞直到列表中有元素后从列表右边弹出元素,阻塞超时后返回null
	 * 该方法是阻塞的
	 * @param key 键
	 * @param maxWait 最大等待时间
	 * @param unit 最大等待时间单位
	 * @return 弹出的对象
	 */
	public Object rightPop(String key,Long maxWait, TimeUnit unit){
		return listOperations.rightPop(key, maxWait, unit);
	}
	
	/**
	 * 阻塞直到列表中有元素后从列表右边弹出元素,阻塞超时后返回null
	 * 该方法是阻塞的
	 * @param key 键
	 * @param maxWaitMills 最大等待时间(毫秒)
	 * @param returnType 返回类型
	 * @return 泛化类型
	 */
	public <R> R rightPop(String key,Long maxWaitMills, Class<R> returnType){
		Object value=listOperations.rightPop(key, maxWaitMills, TimeUnit.MILLISECONDS);
		return returnType.cast(value);
	}
	
	/**
	 * 阻塞直到列表中有元素后从列表右边弹出元素,阻塞超时后返回null
	 * 该方法是阻塞的
	 * @param key 键
	 * @param maxWaitMills 最大等待时间
	 * @param unit 最大等待时间单位
	 * @param returnType 返回类型
	 * @return 泛化类型
	 */
	public <R> R rightPop(String key,Long maxWaitMills, TimeUnit unit,Class<R> returnType){
		Object value=listOperations.rightPop(key, maxWaitMills, unit);
		return returnType.cast(value);
	}
	
	/**
	 * 从列表srcKey右边弹出元素,并将弹出的元素从左边推入列表dstKey中
	 * 该方法是不会阻塞的
	 * @param srcKey 源表键
	 * @param dstKey 目标表键
	 * @return 弹出的对象
	 */
	public Object rightPopAndLeftPush(String srcKey,String dstKey){
		return listOperations.rightPopAndLeftPush(srcKey, dstKey);
	}
	
	/**
	 * 从列表srcKey右边弹出元素,并将弹出的元素从左边推入列表dstKey中
	 * 该方法是不会阻塞的
	 * @param srcKey 源表键
	 * @param dstKey 目标表键
	 * @param returnType 返回类型
	 * @return 泛化类型
	 */
	public <R> R rightPopAndLeftPush(String srcKey,String dstKey,Class<R> returnType){
		Object value=listOperations.rightPopAndLeftPush(srcKey, dstKey);
		return returnType.cast(value);
	}
	
	/**
	 * 阻塞直到列表srcKey中有元素后从列表srcKey右边弹出元素,并将弹出的元素从左边推入列表dstKey中
	 * 该方法是阻塞的
	 * @param srcKey 源表键
	 * @param dstKey 目标表键
	 * @param maxWaitMills 最大等待时间
	 * @param unit 最大等待时间单位
	 * @return 弹出的对象
	 */
	public Object rightPopAndLeftPush(String srcKey,String dstKey,Long maxWaitMills, TimeUnit unit){
		return listOperations.rightPopAndLeftPush(srcKey, dstKey, maxWaitMills, unit);
	}
	
	/**
	 * 阻塞直到列表srcKey中有元素后从列表srcKey右边弹出元素,并将弹出的元素从左边推入列表dstKey中
	 * 该方法是阻塞的
	 * @param srcKey 源表键
	 * @param dstKey 目标表键
	 * @param maxWaitMills 最大等待时间
	 * @param unit 最大等待时间单位
	 * @param returnType 返回类型
	 * @return 弹出的对象
	 */
	public <R> R rightPopAndLeftPush(String srcKey,String dstKey,Long maxWaitMills, TimeUnit unit,Class<R> returnType){
		Object value=listOperations.rightPopAndLeftPush(srcKey, dstKey, maxWaitMills, unit);
		return returnType.cast(value);
	}
	
	/**
     * 计算排重集合的尺寸(元素数量)
     * @param key 集合标识键
     * @return 集合尺寸
     */
    public long setSize(String key, Object value) {
        return setOperations.size(key);
    }
	
	/**
     * 向键映射的排重集合中添加值
     * @param key 键
     * @param value 值
     * @return 被加入到Set集合中的元素数量
     */
    public long setAdd(String key, Object... values) {
        return setOperations.add(key, values);
    }
    
    /**
     * 判断给定键映射的排重集合中是否存在指定的对象
     * @param key 键
     * @param value 值
     * @return 集合中是否存在指定的参数value
     */
    public boolean contains(String key, Object value) {
        return setOperations.isMember(key, value);
    }
    
    /**
     * 移除键映射的排重集合中的值
     * @param key 键
     * @param value 值
     * @return 被移除的元素数量
     */
    public long setDel(String key, Object... values) {
        return setOperations.remove(key, values);
    }
    
    /**
     * 将集合中的参数元素移动到另一个参数集合中去
     * @param srckey 键
     * @param srcvalue 值
     * @param dstkey 键
     * @return 被移除的元素数量
     */
    public boolean setDel(String srckey, Object srcvalue,String dstkey) {
    	return setOperations.move(srckey, srcvalue, dstkey);
    }
    
    /**
     * 获取键映射的排重集合
     * @param key 键
     * @return Set集合对象
     */
    public Set<Object> setGet(String key) {
        return setOperations.members(key);
    }
    
	/**
	 * 对所有key标记的集合取交集
	 * @param key 键
	 * @param otherKeys 其它键集
	 * @return 交集
	 */
	public Set<Object> sinter(String key,String... otherKeys){
		if(null==otherKeys || 0==otherKeys.length) return null;
		return setOperations.intersect(key, Arrays.asList(otherKeys));
	}
	
	/**
	 * 对所有key标记的集合取交集,并将交集结果存储到dstKey标记的集合中
	 * @param dstKey 存储的目标集合
	 * @param key 键
	 * @param otherKeys 其它键集
	 */
	public void sinterStore(String dstKey,String key,String... otherKeys){
		if(null==otherKeys || 0==otherKeys.length) return;
		setOperations.intersectAndStore(key, Arrays.asList(otherKeys), dstKey);
	}
	
	/**
	 * 对所有key标记的集合取并集,并将并集结果存储到dstKey标记的集合中
	 * @param key 键
	 * @param otherKeys 其它键集
	 * @return 并集
	 */
	public Set<Object> union(String key,String... otherKeys){
		if(null==otherKeys || 0==otherKeys.length) return null;
		return setOperations.union(key, Arrays.asList(otherKeys));
	}
	
	/**
	 * 对所有key标记的集合取并集,并将并集结果存储到dstKey标记的集合中
	 * @param dstKey 存储的目标集合
	 * @param key 键
	 * @param otherKeys 其它键集
	 */
	public void unionStore(String dstKey,String key,String... otherKeys){
		if(null==otherKeys || 0==otherKeys.length) return;
		setOperations.unionAndStore(key, Arrays.asList(otherKeys), dstKey);
	}
	
	/**
	 * 从key开始依次对下一个key标记的集合取差集
	 * @param key 键
	 * @param otherKeys 其它键集
	 * @return 差集
	 */
	public Set<Object> diff(String key,String... otherKeys){
		if(null==otherKeys || 0==otherKeys.length) return null;
		return setOperations.difference(key, Arrays.asList(otherKeys));
	}
	
	/**
	 * 从key开始依次对下一个key标记的集合取差集,并将差集结果存储到dstKey标记的集合中
	 * @param dstKey 存储的目标集合
	 * @param key 键
	 * @param otherKeys 其它键集
	 */
	public void diffStore(String dstKey,String key,String... otherKeys){
		if(null==otherKeys || 0==otherKeys.length) return;
		setOperations.differenceAndStore(key, Arrays.asList(otherKeys), dstKey);
	}
	
	/**
	 * 获取有序集合的尺寸(元素数量)
	 * @param key 键
	 * @return 有序集合尺寸
	 */
	public long zsetSize(String key){
		return zsetOperations.size(key);
	}
	
	/**
	 * 往有序集合中添加元素值
	 * 加入有序集合时自动按score值进行升序排列,排序完成后生成索引,索引即为排名的名次
	 * @param key 键
	 * @param value 值
	 * @param score 分数
	 */
	public boolean zsetAdd(String key,Object value,Double score){
		return zsetOperations.add(key, value, score);
	}
	
	/**
	 * 往有序集合中添加元素值
	 * 加入有序集合时自动按score值进行升序排列,排序完成后生成索引,索引即为排名的名次
	 * @param key 键
	 * @param valueDict 元素字典
	 * @return 添加成功的元素数量
	 */
	public long zsetAdd(String key,Set<TypedTuple<Object>> valueSet){
		return zsetOperations.add(key, valueSet);
	}
	
	/**
	 * 往有序集合中添加元素值
	 * 加入有序集合时自动按score值进行升序排列,排序完成后生成索引,索引即为排名的名次
	 * @param key 键
	 * @param valueDict 元素字典
	 * @return 添加成功的元素数量
	 */
	public long zsetAdd(String key,Map<Object,Double> valueDict){
		Set<TypedTuple<Object>> entrySet=new HashSet<TypedTuple<Object>>();
		for(Map.Entry<Object, Double> entry:valueDict.entrySet()){
			entrySet.add(new DefaultTypedTuple<Object>(entry.getKey(),entry.getValue()));
		}
		return zsetAdd(key, entrySet);
	}
	
	/**
	 * 移除有序集合中的元素值
	 * @param key 键
	 * @param values 元素值
	 * @param 移除成功的元素数量
	 */
	public long zsetDel(String key,Object... values){
		return zsetOperations.remove(key, values);
	}
	
	/**
	 * 移除有序集合中指定索引(名次)区间内的元素值
	 * @param key 键
	 * @param start 起始索引
	 * @param end 结束索引
	 * @return 移除成功的元素数量
	 */
	public long zsetDel(String key,Long start,Long end){
		return zsetOperations.removeRange(key, start,end);
	}
	
	/**
	 * 获取有序集合中指定元素值的索引(名次)
	 * @param key 键
	 * @param value 元素值
	 * @param 元素值的排名(索引)
	 */
	public long zsetGetIndex(String key,Object value){
		return zsetOperations.rank(key, value);
	}
	
	/**
	 * 获取有序集合中指定元素值的分数
	 * @param key 键
	 * @param value 元素值
	 * @param 元素值的分数
	 */
	public double zsetGetScore(String key,Object value){
		return zsetOperations.score(key, value);
	}
	
	/**
	 * 获取有序集合中指定score区间内的元素数量
	 * @param key 键
	 * @param min 最小分数
	 * @param max 最大分数
	 * @param score区间中的元素数量
	 */
	public long zsetGetCount(String key,Double min,Double max){
		return zsetOperations.count(key, min,max);
	}
	
	/**
	 * 获取有序集合中指定索引(名次)区间内的元素值
	 * @param key 键
	 * @param start 起始索引
	 * @param end 结束索引
	 * @return 元素值集合
	 */
	public Set<Object> zsetGet(String key,Long start,Long end){
		return zsetOperations.range(key, start,end);
	}
	
	/**
	 * 获取散列表尺寸(元素数量)
	 * @param key 键
	 * @param 散列表尺寸
	 */
	public long hashSize(String key){
		return hashOperations.size(key);
	}
	
	/**
	 * 设置单个键值对到hash表中去
	 * @param key 键
	 * @param field 域
	 * @param value 值
	 */
	public void hashSet(String key,String field,Object value){
		hashOperations.put(key, field, value);
	}
	
	/**
	 * 设置字典中的所有键值对到hash表中去
	 * @param key 键
	 * @param valueMap 字典
	 */
	public void hashSet(String key,Map<String,Object> valueMap){
		hashOperations.putAll(key, valueMap);
	}
	
	/**
	 * 根据键域获取hash表中的值
	 * @param key 键
	 * @param field 域
	 * @return 对象类型
	 * @return 对象
	 */
	public Object hashGet(String key,String field){
		return hashGet(key,field,Object.class);
	}
	
	/**
	 * 根据键域获取hash表中的值
	 * @param key 键
	 * @param field 域
	 * @param type 类型
	 * @return 泛化类型
	 */
	public <R> R hashGet(String key,String field,Class<R> type){
		return type.cast(hashOperations.get(key, field));
	}
	
	/**
	 * 返回键对应的hash表
	 * @param key 键
	 * @return hash表
	 * @return 哈希表
	 */
	public Map<String,Object> hashGet(String key){
		return hashOperations.entries(key);
	}
	
	/**
	 * 获取hash表中所有的值
	 * @param key 键
	 * @return 值列表
	 * @return 哈希表中的值集
	 */
	public List<Object> hashValues(String key){
		return hashOperations.values(key);
	}
	
	/**
	 * 获取hash表中所有的键
	 * @param key 键
	 * @return 哈希表中的域集
	 */
	public Set<String> hashKeys(String key){
		return hashOperations.keys(key);
	}
	
	/**
	 * 删除hash表中的一个键值对
	 * @param key 键
	 * @param fields 域列表
	 */
	public void hashDelete(String key,Object... fields){
		hashOperations.delete(key, fields);
	}
	
	/**
	 * 清空集群中参数节点下的数据库
	 * @param host 节点主机名称(或IP地址)
	 * @param port 节点连接端口
	 */
	public void clearDB(String host,Integer port){
		RedisClusterNode node=RedisClusterNode.newRedisClusterNode().listeningAt(host, port).build();
		clusterOperations.flushDb(node);
	}
	
	/**
	 * 获取Redis集群中的所有主节点
	 * @return 集群中的所有主节点
	 * @description 单节点连接返回NULL
	 */
	public Set<RedisClusterNode> getMasters(){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		return connection.clusterGetMasterSlaveMap().keySet();
	}
	
	/**
	 * 返回参数主节点的所有副本节点
	 * @param host 节点主机名称(或IP地址)
	 * @param port 节点连接端口
	 * @return 参数主节点对应的所有副本节点
	 * @description 单节点连接返回NULL
	 */
	public Collection<RedisClusterNode> getSlaves(String host,Integer port){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		RedisClusterNode node=RedisClusterNode.newRedisClusterNode().listeningAt(host, port).build();
		return connection.clusterGetSlaves(node);
	}
	
	/**
	 * 获取Redis集群中的所有节点
	 * @return 集群中所有节点(包括所有主节点和所有从节点)
	 * @description 单节点连接返回NULL
	 */
	public Iterable<RedisClusterNode> getAllNodes(){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		return connection.clusterGetNodes();
	}
	
	/**
	 * 获取槽位在Redis集群中所处的节点
	 * @param slot 键槽
	 * @return 集群节点
	 * @description 单节点连接返回NULL
	 */
	public RedisClusterNode getNode(Integer slot){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		return connection.clusterGetNodeForSlot(slot);
	}
	
	/**
	 * 获取键在Redis集群中所处的节点
	 * @param key 键
	 * @return 集群节点
	 * @description 单节点连接返回NULL
	 */
	public RedisClusterNode getNode(String key){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		return connection.clusterGetNodeForKey(key.getBytes());
	}
	
	/**
	 * 获取键在Redis集群中所处的节点
	 * @param key 键
	 * @param charset 键编码
	 * @return 集群节点
	 * @description 单节点连接返回NULL
	 */
	public RedisClusterNode getNode(String key,String charset){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		try {
			return connection.clusterGetNodeForKey(key.getBytes(charset));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 获取键在Redis集群中所处的槽位
	 * @param key 键
	 * @return 键槽
	 * @description 单节点连接返回NULL
	 */
	public Integer getSlot(String key){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		return connection.clusterGetSlotForKey(key.getBytes());
	}
	
	/**
	 * 获取键在Redis集群中所处的槽位
	 * @param key 键
	 * @param charset 键编码
	 * @return 键槽
	 * @description 单节点连接返回NULL
	 */
	public Integer getSlot(String key,String charset){
		RedisClusterConnection connection=getRedisClusterConnection();
		if(null==connection) return null;
		try {
			return connection.clusterGetSlotForKey(key.getBytes(charset));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 在指定的节点上查找参数pattern匹配的所有键
	 * @param host 节点主机名称(或IP地址)
	 * @param port 节点连接端口
	 * @param pattern 查找的键串格式
	 * @return 键集
	 */
	public Set<String> keys(String host,Integer port,String pattern){
		RedisClusterNode node=RedisClusterNode.newRedisClusterNode().listeningAt(host, port).build();
		return clusterOperations.keys(node, pattern);
	}
	
	/**
	 * 获取Redis集群连接
	 * @return Redis集群连接
	 * @description 如果是单点连接则返回NULL
	 */
	public RedisClusterConnection getRedisClusterConnection(){
		RedisConnection connection=connection();
		if(RedisClusterConnection.class.isInstance(connection)) return (RedisClusterConnection)connection;
		return null;
	}
	
	/**
	 * 获取Redis单点连接
	 * @return Redis单点连接
	 * @description 如果是集群连接则返回NULL
	 */
	public StringRedisConnection getSingleConnection(){
		RedisConnection connection=connection();
		if(StringRedisConnection.class.isInstance(connection)) return (StringRedisConnection)connection;
		return null;
	}
	
	/**
	 * 往可靠中间件的默认队列中发送消息对象
	 * @param messages 消息对象数组
	 */
	public void send(Object[] messages){
		if(null==messages||0==messages.length) return;
		leftPush(DEFAULT_QUEUE,messages);
	}
	
	/**
	 * 往可靠中间件的指定队列中发送消息对象
	 * @param queue 队列名称
	 * @param messages 消息对象序列
	 */
	public void send(String queue,Object... messages){
		if(null==queue||null==messages||queue.trim().isEmpty()||0==messages.length) return;
		leftPush(queue,messages);
	}
	
	/**
	 * 往可靠中间件的指定队列中发送消息对象并设置队列过期时间
	 * 默认设置队列的过期时长为10秒
	 * @param queue 队列名称
	 * @param messages 消息对象序列
	 */
	public void sendAndExpire(String queue,Object... messages){
		sendAndExpire(queue,DEFAULT_EXPIRE,messages);
	}
	
	/**
	 * 往可靠中间件的指定队列中发送消息对象并设置队列过期时间
	 * @param queue 队列名称
	 * @param expireMills 过期时长
	 * @param unit 过期时长单位
	 * @param messages 消息对象序列
	 */
	public void sendAndExpire(String queue,Long expireMills,Object... messages){
		if(null==queue||null==messages||null==expireMills||0==expireMills||queue.trim().isEmpty()||0==messages.length) return;
		leftPush(queue,messages);
		expire(queue,expireMills,TimeUnit.MILLISECONDS);
	}
	
	/**
	 * 往可靠中间件的指定队列中发送消息对象同时从指定队列中接收数据返回
	 * 若超过10秒未接收到数据则默认返回NULL
	 * @param pushQueue 发送数据的目标队列
	 * @param pullQueue 接收数据的目标队列
	 * @param messages 发送的消息序列
	 * @return 接收的消息对象
	 */
	public Object sendAndReceive(String pushQueue,String pullQueue,Object... messages){
		return sendAndReceive(pushQueue,pullQueue,QUEUE_MAX_PULL_TIMEOUT,Object.class,messages);
	}
	
	/**
	 * 往可靠中间件的指定队列中发送消息对象同时从指定队列中接收数据返回
	 * 该方法将最长阻塞到超时时间为止
	 * @param pushQueue 发送数据的目标队列
	 * @param pullQueue 接收数据的目标队列
	 * @param maxWaitMills 最大阻塞等待时间毫秒数
	 * @param messages 发送的消息序列
	 * @return 接收的消息对象
	 */
	public Object sendAndReceive(String pushQueue,String pullQueue,Long maxWaitMills,Object... messages){
		return sendAndReceive(pushQueue,pullQueue,maxWaitMills,Object.class,messages);
	}
	
	/**
	 * 往可靠中间件的指定队列中发送消息对象同时从指定队列中接收数据返回
	 * 该方法将最长阻塞到超时时间为止
	 * @param pushQueue 发送数据的目标队列
	 * @param pullQueue 接收数据的目标队列
	 * @param returnType 返回类型
	 * @param messages 发送的消息序列
	 * @return 接收的消息对象
	 */
	public <R> R sendAndReceive(String pushQueue,String pullQueue,Class<R> returnType,Object... messages){
		return sendAndReceive(pushQueue,pullQueue,QUEUE_MAX_PULL_TIMEOUT,returnType,messages);
	}
	
	/**
	 * 往可靠中间件的指定队列中发送消息对象同时从指定队列中接收数据返回
	 * 该方法将最长阻塞到超时时间为止
	 * @param pushQueue 发送数据的目标队列
	 * @param pullQueue 接收数据的目标队列
	 * @param maxWaitMills 最大阻塞等待时间毫秒数
	 * @param returnType 返回类型
	 * @param messages 发送的消息序列
	 * @return 接收的消息对象
	 * @description maxWaitMills有以下取值:
	 * maxWaitMills=0:非阻塞,快速返回数据或NULL
	 * maxWaitMills<0:一直阻塞直到有数据返回为止(不推荐)
	 * maxWaitMills>0:阻塞直到有数据返回或阻塞超过maxWaitMills时间返回NULL
	 */
	public <R> R sendAndReceive(String pushQueue,String pullQueue,Long maxWaitMills,Class<R> returnType,Object... messages){
		if(null==pushQueue||null==messages||pushQueue.trim().isEmpty()||0==messages.length) return null;
		if(null==pullQueue||null==maxWaitMills||null==returnType) return null;
		
		leftPush(pushQueue,messages);
		
		if(0==maxWaitMills) return rightPop(pullQueue,returnType);
		
		R r=null;
		long waitTime=0;
		if(0>maxWaitMills)
		while(!Thread.currentThread().isInterrupted()) if(null!=(r=rightPop(pullQueue,QUEUE_MAX_PULL_TIMEOUT,TimeUnit.MILLISECONDS,returnType))) return r;
		
		if(maxWaitMills<=QUEUE_MAX_PULL_TIMEOUT) return rightPop(pullQueue,maxWaitMills,TimeUnit.MILLISECONDS,returnType);
		
		while(!Thread.currentThread().isInterrupted()) {
			r=rightPop(pullQueue,QUEUE_MAX_PULL_TIMEOUT,TimeUnit.MILLISECONDS,returnType);
			if(null==r && maxWaitMills>=(waitTime=waitTime+QUEUE_MAX_PULL_TIMEOUT)) continue;
			break;
		}
		return r;
	}
	
	/**
	 * 往即时中间件的指定通道中发送消息对象
	 * @param channel 通道名称
	 * @param messages 消息对象序列
	 */
	public void convertAndSend(String channel,Object... messages){
		if(null==channel || null==messages || channel.trim().isEmpty() || 0==messages.length) return;
		for(Object message:messages) redisTemplate.convertAndSend(channel, message);
	}
	
	/**
	 * 执行Redis回调
	 * @param callBack 可调对象
	 * @return 调用结果
	 */
	public <R> R execute(RedisCallback<R> callBack){
		return redisTemplate.execute(callBack);
	}
	
	/**
	 * 执行Redis脚本
	 * @param luaScript lua脚本
	 * @param keys 键集
	 * @param args 参数集
	 * @return 脚本调用结果
	 */
	public <R> R execute(RedisScript<R> luaScript, List<String> keys, Object... args) {
		return redisTemplate.execute(luaScript,keys,args);
	}
	
	/**
	 * 获取通用操作模板
	 * @return RedisTemplate
	 */
	public RedisTemplate template(){
		return redisTemplate;
	}
	
	/**
	 * 获取Redis连接工厂
	 * @return RedisConnectionFactory
	 */
	public RedisConnectionFactory factory(){
		return redisTemplate.getConnectionFactory();
	}
	
	/**
	 * 获取Redis连接
	 * @return RedisConnection
	 * @description 根据底层连接实现返回Redis单点连接或Redis集群连接
	 */
	public RedisConnection connection(){
		return factory().getConnection();
	}
	
	/**
	 * 获取Redis哨兵连接
	 * @return RedisSentinelConnection
	 * @description 根据底层连接实现返回哨兵单点连接或哨兵集群连接
	 */
	public RedisSentinelConnection sentinelConnection(){
		return factory().getSentinelConnection();
	}
	
	/**
	 * 获取value操作工具
	 * @return ValueOperations
	 */
	public ValueOperations<String,Object> value(){
		return valueOperations;
	}
	
	/**
	 * 获取list操作工具
	 * @return ListOperations
	 */
	public ListOperations<String,Object> list(){
		return listOperations;
	}
	
	/**
	 * 获取set操作工具
	 * @return SetOperations
	 */
	public SetOperations<String, Object> set(){
		return setOperations;
	}
	
	/**
	 * 获取zset操作工具
	 * @return ZSetOperations
	 */
	public ZSetOperations<String, Object> zset(){
		return zsetOperations;
	}
	
	/**
	 * 获取hash操作工具
	 * @return HashOperations
	 */
	public HashOperations<String, String, Object> hash(){
		return hashOperations;
	}
	
	/**
	 * 获取cluster操作工具
	 * @return ClusterOperations
	 */
	public ClusterOperations<String,Object> cluster(){
		return clusterOperations;
	}
	
	/**
     * 设置分布式锁
     * 在指定键的映射不存在时按指定的过期时长设置键到值的映射
     * @param key 锁key
     * @param value 唯一标识符
     * @param expire 过期时间(单位:秒)
     * @return true 本方法调用前锁已存在,本次加锁失败;false 本方法调用前锁不存在,本次加锁成功
     */
    public boolean hasLock(String key, String value, int expire) {
        try {
            RedisCallback<Boolean> callback = (connection) -> {
                return connection.set(key.getBytes(Charset.forName("UTF-8")),
                		value.getBytes(Charset.forName("UTF-8")),
                        Expiration.seconds(TimeUnit.SECONDS.toSeconds(expire)),
                        RedisStringCommands.SetOption.SET_IF_ABSENT);
            };
            return execute(callback)?false:true;
        } catch (Exception e) {
            e.printStackTrace();
            return true;
        }
    }
    
    /**
     * 释放分布式锁
     * 如果指定键的映射值与给定参数值相等则从缓存中移除该键的映射
     * @param key 键
     * @param value 值
     * @return false 解锁失败,true 解锁成功
     */
    public boolean releaseLock(String key, String value) {
    	Boolean flag=null;
		String functionName=funToScriptDict.get("UNLOCK_LUA");
		if(null!=functionName) {
			try{
				RedisCallback<Boolean> redisCallback=connection->connection.evalSha(functionName, ReturnType.BOOLEAN, 1, key.getBytes(Charset.forName("UTF-8")),value.getBytes(Charset.forName("UTF-8")));
				flag=execute(redisCallback);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		if(null!=flag) return flag;
		DefaultRedisScript<Boolean> redisScript = new DefaultRedisScript(UNLOCK_LUA,Boolean.class);
		funToScriptDict.put("UNLOCK_LUA", redisScript.getSha1());
		return execute(redisScript, Arrays.asList(key), Arrays.asList(value));
    }
    
    /**
     * 串行化(同步)执行指定任务
     * 备注:除非有特别的理由来说明某个任务的执行时间不能超过某个阈值,否则过期时间应尽可能的设置的大一些更好,这样可以确保分布式锁不被穿透
     * @param task 任务
     * @param key 需要加锁的KEY
     * @param value 需要加锁的VALUE
     * @param expire 过期时间(单位:秒)
     */
    public void execute(Runnable task,String key, String value,int... expires) {
    	int expire=null==expires || 0==expires.length ? DEFAULT_LOCK_TIMEOUT : expires[0];
    	while(hasLock(key, value, expire)){
    		try {
				Thread.sleep(TRY_LOCK_INTERVAL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    	
    	try {
    		task.run();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			releaseLock(key, value);
		}
    }
    
    /**
     * 串行化(同步)执行指定任务
     * 备注:除非有特别的理由来说明某个任务的执行时间不能超过某个阈值,否则过期时间应尽可能的设置的大一些更好,这样可以确保分布式锁不被穿透
     * @param task 任务
     * @param key 需要加锁的KEY
     * @param value 需要加锁的VALUE
     * @param expire 过期时间(单位:秒)
     * @return 泛化类型 
     */
    public <R> R execute(Callable<R> task,String key, String value,int... expires) {
    	int expire=null==expires || 0==expires.length ? DEFAULT_LOCK_TIMEOUT : expires[0];
    	while(hasLock(key, value, expire)){
    		try {
				Thread.sleep(TRY_LOCK_INTERVAL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    	
    	R r = null;
    	try {
			r=task.call();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			releaseLock(key, value);
		}
    	return r;
    }
    
    /**
     * 串行化(同步)执行指定任务
     * 备注:除非有特别的理由来说明某个任务的执行时间不能超过某个阈值,否则过期时间应尽可能的设置的大一些更好,这样可以确保分布式锁不被穿透
     * @param task 任务
     * @param key 需要加锁的KEY
     * @param value 需要加锁的VALUE
     * @param expire 过期时间(单位:秒)
     * @param t 执行任务的参数列表
     * @return 泛化类型 
     */
    public <T,R> R execute(Executable<T,R> task,String key, String value,int expire,T... t) {
    	while(hasLock(key, value, expire)){
    		try {
				Thread.sleep(TRY_LOCK_INTERVAL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    	
    	R r = null;
    	try {
			r=task.execute(t);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			releaseLock(key, value);
		}
    	return r;
    }
    
    /**
     * @author Louis
     * @param <T> 入参类型
     * @param <R> 出值类型
     * @description 可执行接口,该接口是对Runnable接口和Callable接口的参数扩展
     */
    public interface Executable<T,R>{
    	/**
    	 * 任务执行方法
    	 * @param t 参数对象
    	 * @return 出值对象
    	 */
    	public R execute(T... t);
    }
}
