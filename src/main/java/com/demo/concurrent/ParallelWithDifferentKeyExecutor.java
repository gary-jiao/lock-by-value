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
public class ParallelWithDifferentKeyExecutor<T, M> {
	
	private static Logger logger = LoggerFactory.getLogger(ParallelWithDifferentKeyExecutor.class);
	
	private volatile static ConcurrentMap<String, Object> workList = new ConcurrentHashMap<>();
	
	private ThreadPoolExecutor executor = null;
	private Queue<DifferentKeyThreadWorker<T, M>> keyList = new ConcurrentLinkedQueue<>();
	private Lock lock = new ReentrantLock();
	private Boolean isRunning = false;
	private List<Future<T>> resultList = new ArrayList<>();
	
	public ParallelWithDifferentKeyExecutor() {
		//如果不指定线程数量，就默认取CPU的核数
		this(Runtime.getRuntime().availableProcessors());
	}

	public ParallelWithDifferentKeyExecutor(int threadCount) {
		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
	}
	
	public ParallelWithDifferentKeyExecutor<T, M> addWorkers(List<M> dataList, KeyMaker<M> maker, ThreadWorker<T, M> threadWorker) {
		keyList.addAll(dataList.parallelStream().map(data -> new DifferentKeyThreadWorker<T, M>(maker.toKey(data), data, threadWorker){}).collect(Collectors.toList()));
		return this;
	}

	/**
	 * @param worker
	 * @return
	 */
	public ParallelWithDifferentKeyExecutor<T, M> addWorker(DifferentKeyThreadWorker<T, M> worker) {
		Assert.notNull(worker.getKey(), "The key cannot be null");
		Assert.hasLength(worker.getKey(), "Key must not be empty");
		keyList.add(worker);
		return this;
	}

	protected ParallelWithDifferentKeyExecutor<T, M> run() {
		try {
			lock.lock();
			if (isRunning) return this;
			isRunning = true;
		} finally {
			lock.unlock();
		}
		for (;;) {
			Iterator<DifferentKeyThreadWorker<T, M>> ite = keyList.iterator();
			while (ite.hasNext()) {
				DifferentKeyThreadWorker<T, M> data = ite.next();

				lock.lock();
				if (!workList.containsKey(data.getKey())) {
					workList.putIfAbsent(data.getKey(), data);
					resultList.add(executor.submit(data));
					ite.remove();
				}
				lock.unlock();
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
	
	public List<Future<T>> getResults() {
		logger.info("The size of keyList is: " + keyList.size());
		if (!keyList.isEmpty()) run();
		while (!keyList.isEmpty()) ;
		executor.shutdown();
		return this.resultList;
	}

	public static abstract class DifferentKeyThreadWorker<T, M> implements Callable<T> {

		private String key;
		private M data;
		private ThreadWorker<T, M> worker;

		public DifferentKeyThreadWorker(String key, M data, ThreadWorker<T, M> worker) {
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
		
		public T call() throws Exception {
			try {
				return worker.runWork(data);
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
	
	public interface ThreadWorker<T, M> {
		T runWork(M data);
	}
	
	public interface KeyMaker<M> {
		String toKey(M object);
	}
}
