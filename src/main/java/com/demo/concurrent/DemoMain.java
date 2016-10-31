package com.demo.concurrent;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.demo.concurrent.KeyLocker.KeyLockerWorkThread;

public class DemoMain {
	
	public static void main(String[] args) throws Exception {
		List<String> phoneList = new ArrayList<>();
		phoneList.add("13900000000");
		phoneList.add("13911111111");
//		phoneList.add("13922222222");
//		phoneList.add("13933333333");
//		phoneList.add("13944444444");
//		phoneList.add("13955555555");
		KeyLocker kl = KeyLocker.getInstance(5);
		kl.start();
		
		for (int i = 0; i < 10; i++) {
			new Thread() {
				public void run() {
//					waitThread(5);
					for (String phone : phoneList) {
//						waitThread(2);
						KeyLockerWorkThread thread = new KeyLockerWorkThread() {
							@Override
							public void runWork() {
								System.out.println(Thread.currentThread().getName() + " , " + Clock.systemUTC().millis() + " : working for , " + phone);
								waitFixThread(2);
							}
						};
						kl.addWorker(phone, thread);
					}
				}
			}.start();
		}
		
		Thread.sleep(10000000);
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
