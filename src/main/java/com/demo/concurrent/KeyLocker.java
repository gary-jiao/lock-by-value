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
 * 根据传入主键的值进行判断，如果传入的值已经在运行中，则进行等待，否则启动运行
 * 
 * @author gary
 *
 */
public class KeyLocker extends Thread {

	private ThreadPoolExecutor executor = null;
	private volatile static ConcurrentMap<String, Object> workList = new ConcurrentHashMap<>();
	private volatile Queue<KeyObjectMap> keyList = new ConcurrentLinkedQueue<>();
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

	public KeyLocker addWorker(String key, KeyLockerWorkThread thread) {
		Assert.notNull(key, "The key cannot be null");
		Assert.hasLength(key, "Key must not be empty");

		keyList.add(new KeyObjectMap(key, thread));

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
					ite.remove();
					executor.execute(data.getData());
				}
				lock.unlock();
			}
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
			}
		}
	}

	private class KeyObjectMap {
		private String key;
		private KeyLockerWorkThread data;

		public KeyObjectMap(String key, KeyLockerWorkThread data) {
			this.key = key;
			this.data = data;
			this.data.setKey(key);
		}

		public String getKey() {
			return key;
		}

		public KeyLockerWorkThread getData() {
			return data;
		}

	}

	public static abstract class KeyLockerWorkThread extends Thread {

		private String key;

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

		public abstract void runWork();
	}
}
