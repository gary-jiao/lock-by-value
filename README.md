根据某个键值限制并发操作
=====================

我们日常使用到的并发操作，一般都是根据线程数量来决定并发操作，但某些时候，希望能够尽量并发，但如果发现对象的某个值是一样的，则对于同样值的对象采用串行处理，这样即能使用并发增加处理能力，也能够避免对同一个值并发操作产生的问题。

- ParallelWithDifferentKeyExecutor  
	每次循环里，都会把没有重复的数据提交处理，所有使用的无限while循环的方式，一直到集合里没有数据为止。因为没有使用到Java8的特性，代码理论上应该可以在Java5以上的平台运行。
	但因为开发是在Java8下，如果在8以下版本使用，代码里可能要做些微修改。
	
- ParallelWithDifferentKeyStreamExecutor  
	使用Java8的lambda写法以及CompletableFuture来完成功能，代码上更精简，但限制运行平台必须是Java8以上。
	