public class TaskResult {

	//任务是否回滚失败
	public Boolean rollbackFail;
	//任务的事务对象
	public ServiceTransaction trans;
	//任务异常
	public Exception be;
}
