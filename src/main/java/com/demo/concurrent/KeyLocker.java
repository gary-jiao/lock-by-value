package com.demo.concurrent;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.util.Assert;

/**
 * <pre>
 * Java正常的并发操作一般都是限制并发数量，如果是希望对某个值进行并发限制，目前没有找到已有的方法。
 * 所以实现以下代码，根据传入主键的值进行判断，如果传入的值已经在运行中，则进行等待，否则启动运行。
 * 也就是对于不同的键值，是处于并发操作的，但对于相同键值，是需要串行处理的
 * </pre>
 * 
 * @author gary
 *
 */
public class KeyLocker extends Thread {

	private static ThreadPoolExecutor executor = null;
	private volatile static ConcurrentMap<String, Object> workList = new ConcurrentHashMap<>();
	private volatile static Queue<KeyObjectMap> keyList = new ConcurrentLinkedQueue<>();
	private static Lock lock = new ReentrantLock();

	private static KeyLocker instance = null;

	private KeyLocker(int threadCount) {
		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
	}

	public static KeyLocker getInstance(int threadCount) {
		Assert.state(threadCount > 0, "the thread count must greater than 0");

		if (instance == null) {
			lock.lock();
			if (instance == null) {
				instance = new KeyLocker(threadCount);
			}
			lock.unlock();
		}

		return instance;
	}

	public KeyLocker addWorker(KeyLockerThreadWorker worker) {
		Assert.notNull(worker.getKey(), "The key cannot be null");
		Assert.hasLength(worker.getKey(), "Key must not be empty");
		keyList.add(new KeyObjectMap(worker.getKey(), worker));
		return this;
	}

	public void run() {
		while (true) {
			Iterator<KeyObjectMap> ite = keyList.iterator();
			while (ite.hasNext()) {
				KeyObjectMap data = ite.next();

				lock.lock();
				if (!workList.containsKey(data.getKey())) {
					workList.putIfAbsent(data.getKey(), data.getData());
					executor.execute(data.getData());
					ite.remove();
				}
				lock.unlock();
			}
			try {
				Thread.sleep(20);
				System.out.println("Sleep 20......");
			} catch (InterruptedException e) {
			}
		}
	}

	private class KeyObjectMap {
		private String key;
		private KeyLockerThreadWorker data;

		public KeyObjectMap(String key, KeyLockerThreadWorker data) {
			this.key = key;
			this.data = data;
			this.data.setKey(key);
		}

		public String getKey() {
			return key;
		}

		public KeyLockerThreadWorker getData() {
			return data;
		}

	}

	public static abstract class KeyLockerThreadWorker extends Thread {

		private String key;

		public KeyLockerThreadWorker(String key) {
			this.key = key;
		}
		
		public KeyLockerThreadWorker(KeyMaker keyMaker) {
			this(keyMaker.toKey());
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public void run() {
			try {
				runWork();
			} finally {
				releaseWork();
			}
		}

		public void releaseWork() {
			workList.remove(key);
		}

		protected abstract void runWork();
	}
	
	public interface KeyMaker {
		String toKey();
	}
}
