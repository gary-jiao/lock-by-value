package com.demo.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class ParallelWithDifferentKeyExecutor<M, R> {
	
	private static Logger logger = LoggerFactory.getLogger(ParallelWithDifferentKeyExecutor.class);
	
	private volatile static ConcurrentMap<String, Object> workList = new ConcurrentHashMap<>();
	
	private ThreadPoolExecutor executor = null;
	private Queue<DifferentKeyThreadWorker<M, R>> keyList = new ConcurrentLinkedQueue<>();
	private Lock lock = new ReentrantLock();
	private Boolean isRunning = false;
	private List<Future<R>> resultList = new ArrayList<>();
	
	public ParallelWithDifferentKeyExecutor() {
		//如果不指定线程数量，就默认取CPU核数的2倍
		this(Runtime.getRuntime().availableProcessors() * 2);
	}

	public ParallelWithDifferentKeyExecutor(int threadCount) {
		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
	}
	
	public ParallelWithDifferentKeyExecutor<M, R> addWorkers(List<M> dataList, Function<M, String> maker, Function<M, R> threadWorker) {
		keyList.addAll(dataList.parallelStream().map(data -> new DifferentKeyThreadWorker<M, R>(maker.apply(data), data, threadWorker){}).collect(Collectors.toList()));
		return this;
	}

	/**
	 * @param worker
	 * @return
	 */
	public ParallelWithDifferentKeyExecutor<M, R> addWorker(DifferentKeyThreadWorker<M, R> worker) {
		Assert.notNull(worker.getKey(), "The key cannot be null");
		Assert.hasLength(worker.getKey(), "Key must not be empty");
		keyList.add(worker);
		return this;
	}

	protected ParallelWithDifferentKeyExecutor<M, R> run() {
		try {
			lock.lock();
			if (isRunning) return this;
			isRunning = true;
		} finally {
			lock.unlock();
		}
		for (;;) {
			Iterator<DifferentKeyThreadWorker<M, R>> ite = keyList.iterator();
			while (ite.hasNext()) {
				DifferentKeyThreadWorker<M, R> data = ite.next();
				
				//使用Java自带的保证原子操作的方法完成，避免使用锁
				workList.computeIfAbsent(data.getKey(), k -> {
					resultList.add(executor.submit(data));
					ite.remove();
					return data; 
				});
			}
			if (keyList.isEmpty()) break;
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
			}
		}
		isRunning = false;
		return this;
	}
	
	public List<Future<R>> getResults() {
		logger.info("The size of keyList is: " + keyList.size());
		if (!keyList.isEmpty()) run();
		while (!keyList.isEmpty()) ;
		executor.shutdown();
		return this.resultList;
	}

	public static abstract class DifferentKeyThreadWorker<M, R> implements Callable<R> {

		private String key;
		private M data;
		private Function<M, R> worker;

		public DifferentKeyThreadWorker(String key, M data, Function<M, R> worker) {
			this.key = key;
			this.data = data;
			this.worker = worker;
		}

		public String getKey() {
			return key;
		}
		
		public M getData() {
			return this.data;
		}
		
		public R call() throws Exception {
			try {
				return worker.apply(data);
			} finally {
				releaseWork();
			}
		}

		public void releaseWork() {
			workList.remove(key);
		}

		@Override
		public String toString() {
			return "DifferentKeyThreadWorker [key=" + key + ", data=" + data + ", worker=" + worker + "]";
		}
	}
}
