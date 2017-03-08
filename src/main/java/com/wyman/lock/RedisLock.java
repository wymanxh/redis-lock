package com.wyman.lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

import static java.lang.String.format;

/**
 * Created by root on 3/8/17.
 */

public abstract class RedisLock {

	public static String acquireLock(Jedis jedis, String lockName, long lockTimeout) {
		return acquireLock(jedis, lockName, 10000L, lockTimeout);
	}

	public static String acquireLock(Jedis jedis, String lockName, long acquireTimeout, long lockTimeout) {
		String key = format("lock:%s", lockName);
		long end = System.currentTimeMillis() + acquireTimeout;
		String identifier = UUID.randomUUID().toString();

		int lockExpire = (int) (lockTimeout / 1000);

		while (System.currentTimeMillis() < end) {
			if (jedis.setnx(key, identifier) == 1) {
				jedis.expire(key, lockExpire);

				// 成功获取锁，返回identifier
				return identifier;
			}

			// 即使没有获取到锁，仍然检查下锁有没有设置过期时间，没有就重设过期时间。避免程序在setnx和expire之间崩溃，导致锁永远不能释放的情况
			if (jedis.ttl(key) == -1) {
				jedis.expire(key, lockExpire);
			}

			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// 超时没有获取到锁
		return null;
	}

	public static boolean releaseLock(Jedis jedis, String lockName, String identifier) {
		String key = format("lock:%s", lockName);
		while (true) {
			jedis.watch(key);
			// 判断下identifier，防止重复删除
			if (identifier.equals(jedis.get(key))) {
				Transaction tx = jedis.multi();
				tx.del(key);
				List<Object> exec = tx.exec();
				// 锁的值被修改了，终止事务
				if (exec == null) {
					continue;
				}

				// 成功释放锁
				return true;
			}

			jedis.unwatch();
			break;
		}

		// 锁已经丢失了
		return false;
	}
}
