package com.demo.concurrent;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.demo.concurrent.DemoMain.User;
import com.demo.concurrent.ParallelWithDifferentKeyExecutor.DifferentKeyThreadWorker;

public class DemoMain {
	
	public static void main(String[] args) throws Exception {
		test2();
	}
	
	/**
	 * 想要使用Java8自带的Lambda写法来完成重复号码在同一线程内操作的功能，但貌似还没有实现
	 * @throws Exception
	 */
	private static void test3() throws Exception {
		int m = 1;
		List<User> userList = Arrays.asList(
									new User(m++, "Hello", "111"),
									new User(m++, "Hello1", "222"),
									new User(m++, "Hello2", "222"),
									new User(m++, "Hello3", "222"),
									new User(m++, "Hello4", "111"),
									new User(m++, "Hello5", "222"),
									new User(m++, "Hello6", "222"),
									new User(m++, "Hello7", "111")
								);
		
		userList.parallelStream()
					 .collect(Collectors.groupingBy(User::getMobile))
					 .entrySet()
					 	.parallelStream()			//本意是希望这里产生多个线程
					 	.forEach(entry -> {
					 		entry.getValue()
						 		//这里本来不想使用parallelStream，通过前面产生的多线程，在每个线程内部单线程运行
								//但实际运行下来，这里如果使用stream，则所有数据只是在单线程内运行了
					 				.parallelStream()
					 				.forEach(user -> {
					 					try {
					 						Thread.sleep(new Random().nextInt(5) * 1000);
					 					} catch (InterruptedException e) {
					 					}
					 					System.out.println(Thread.currentThread().getName() + " , " + Clock.systemUTC().millis() + " : working for , " + user.getId() + " / " + user.getUsername() + " / " + user.getMobile());
					 				});
					 	});
	}
	
	private static void test2() throws Exception {
		int m = 1;
		List<User> userList = Stream.of(
									new User(m++, "Hello", "111"),
									new User(m++, "Hello1", "222"),
									new User(m++, "Hello2", "222"),
									new User(m++, "Hello3", "222"),
									new User(m++, "Hello4", "111"),
									new User(m++, "Hello5", "222"),
									new User(m++, "Hello6", "222"),
									new User(m++, "Hello7", "111"),
									new User(m++, "Hello7", "111"),
									new User(m++, "Hello7", "111"),
									new User(m++, "Hello7", "111"),
									new User(m++, "Hello7", "111"),
									new User(m++, "Hello7", "111"),
									new User(m++, "Hello7", "111"),
									new User(m++, "Hello7", "111")
								).collect(Collectors.toList());
		
		ParallelWithDifferentKeyExecutor<Integer, User> kl = new ParallelWithDifferentKeyExecutor<>(5);
		kl.addWorkers(userList, User::getMobile, user -> {
			try {
				System.out.println("Start: " + Thread.currentThread().getName() + " , " + Clock.systemUTC().millis() + " : working for , " + user.getId() + " / " + user.getMobile());
				Thread.sleep(new Random().nextInt(5) * 1000);
				return user.getId();
			} catch (InterruptedException e) {
			} finally {
				System.out.println("End: " + Thread.currentThread().getName() + " , " + Clock.systemUTC().millis() + " : working for , " + user.getId() + " / " + user.getMobile());
			}
			return user.getId();
		});
		for (Future<Integer> future : kl.getResults()) {
			try { 
				System.out.println(future.get());
			} catch (ExecutionException ex) {
				ex.printStackTrace();
				//如果希望遇到错误就取消所有任务，则可以执行以下注释掉的代码
//				for(Future f : kl.getResults()){
//		            f.cancel(true);
//		        }
//		        kl.getResults().clear();
			}
		}
	}
	
	static class User {
		private int id;
		private String mobile;
		private String username;
		
		public User(int id, String username, String mobile) {
			this.id = id;
			this.username = username;
			this.mobile = mobile;
		}
		
		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getMobile() {
			return mobile;
		}
		public void setMobile(String mobile) {
			this.mobile = mobile;
		}
		public String getUsername() {
			return username;
		}
		public void setUsername(String username) {
			this.username = username;
		}
		
	}
	
	private static <T> void test() {
		List<String> phoneList = new ArrayList<>();
		phoneList.add("13900000000");
		phoneList.add("13911111111");
		phoneList.add("13911111111");
		phoneList.add("13900000000");
		phoneList.add("13900000000");
		phoneList.add("13900000000");
		phoneList.add("13900000000");
		phoneList.add("13900000000");
		phoneList.add("13911111111");
		phoneList.add("13900000000");
//		phoneList.add("13922222222");
//		phoneList.add("13933333333");
//		phoneList.add("13944444444");
//		phoneList.add("13955555555");
		ParallelWithDifferentKeyExecutor<String, String> kl = new ParallelWithDifferentKeyExecutor<>(5);
		
		for (String phone : phoneList) {
//			waitThread(2);
			kl.addWorker(new DifferentKeyThreadWorker<String, String>(phone, phone, (phone1) -> {
					System.out.println(Thread.currentThread().getName() + " , " + Clock.systemUTC().millis() + " : working for , " + phone1);
					waitFixThread(2);
					return Thread.currentThread().getName() + " , " + Clock.systemUTC().millis() + " : working for , " + phone1;
				}
			){});
		}
		kl.getResults();
	}
	
	private static void waitThread(int max) {
		try {
			Thread.sleep(new Random().nextInt(max) * 1000);
		} catch (InterruptedException e) {
		}
	}
	
	private static void waitFixThread(int max) {
		try {
			Thread.sleep(max * 1000);
		} catch (InterruptedException e) {
		}
	}
}
