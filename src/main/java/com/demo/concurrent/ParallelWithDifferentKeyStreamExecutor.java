package com.demo.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * ParallelWithDifferentKeyExecutor的改进版本，完全使用Java提供的lambda写法完成，代码比较简练。
 * 而且因为使用的是Java的BlockingQueue，也解决了之前版本里需要用无限循环的问题
 * 
 * 泛型参数说明：
 * M: 需要处理的数据对象类型
 * R: 返回列表（也就是Future）里的数据类型
 * </pre>
 * @author gary
 *
 */
public class ParallelWithDifferentKeyStreamExecutor<M, R> {
	private static Logger logger = LoggerFactory.getLogger(ParallelWithDifferentKeyExecutor.class);
	
	private volatile static ConcurrentMap<String, BlockingQueue<Object>> workList = new ConcurrentHashMap<>();
	private List<CompletableFuture<Object>> resultList = new ArrayList<>();
	private int threadCount = 0;
	
	public ParallelWithDifferentKeyStreamExecutor() {
		//如果不指定线程数量，就默认取CPU核数的2倍
		this(Runtime.getRuntime().availableProcessors() * 2);
	}

	public ParallelWithDifferentKeyStreamExecutor(int threadCount) {
		this.threadCount = threadCount;
	}
	
	public List<CompletableFuture<Object>> addWorkers(List<M> dataList, Function<M, String> keyMaker, Function<M, R> threadWorker) {
		resultList = dataList.stream().map(data -> {
			return CompletableFuture.supplyAsync(() -> {
				BlockingQueue<Object> queue = workList.computeIfAbsent(keyMaker.apply(data), key -> {
					return new ArrayBlockingQueue<>(1);
				});
				try {
					queue.put(data);
				} catch (Exception e) {
					logger.error("", e);
					throw new RuntimeException(e);
				}
				return queue;
			}).thenApplyAsync((queue) -> {
				try {
					M obj = (M)queue.peek();
					return threadWorker.apply(obj);
				} finally {
					queue.clear();
				}
			});
		}).collect(Collectors.toList());
		return resultList;
	}
	
	public List<Object> getResults() {
		return resultList.stream().map(future -> {
			try {
				return future.get();
			} catch (Exception e) {
				logger.error("", e);
				return e;
			}
		}).collect(Collectors.toList());
	}
}
