# forkJoinTask_apply
java7 ForkJoin实战(不完全代码)<br/>
./ICommonServiceObserver.java 事务观察者接口 ===》事务回滚回调<br/>
./IServiceTransaction.java 事务接口 ===》事务回滚<br/>
./ServiceTransaction.java 事务实现类 ===》 事务回滚实现<br/>
./TaskExecutor.java 任务执行器 ===》定义统一的任务调度，结果处理<br/>
./TaskResult.java 任务结果类 ===》封装每个任务的执行结果 <br/>
./VirtualTask.java 抽象任务类 ===》定义统一的任务分割，执行逻辑<br/>
