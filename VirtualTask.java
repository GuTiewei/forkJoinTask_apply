/**
 * 抽象任务类
 * 	定义一些统一方法
 *
 */
public abstract class VirtualTask extends RecursiveTask<List<TaskResult>>{

	protected final Object[] param;  //sdn控制器参数
	
	protected final int start;  //任务单元start index
	
	protected final int end;   //任务单元end index
	
	protected VirtualPortTask(Object[] param, int start, int end) {
		super();
		this.param = param;
		this.start = start;
		this.end = end;
	}
	

	/**
	 * 单个任务单元执行[start,end)
	 */
	@Override
	protected List<TaskResult> compute() {
		// TODO Auto-generated method stub
		
		List<TaskResult> list = new ArrayList<>();
		
		/*
		 * 如果分配的任务单元 > 1，再分割任务单元
		 */
		
		if(end - start > 1) {
			
			int mid = (start + end) / 2 ;	
			List<VirtualPortTask> tasks = new ArrayList<>();
			//拆分任务
			for(VirtualPortTask task : this.splitTasks(start, mid, end)) {
				tasks.add(task);
			}			
			Collection<VirtualPortTask> results = ForkJoinTask.invokeAll(tasks);
			/*
			 * 收集每个任务单元的执行结果
			 */
			for(VirtualPortTask thread : results) {
				 List<TaskResult> resultList = thread.join();
				 list.addAll(resultList);
			}
			return list;
			
		}else {
			for(int index = start; index < end; index++) {
				/*
				 * 每个任务单元创建独立的事务对象
				 */
				ServiceTransaction trans = ServiceCommonUtil.getTransaction();
				try {
					
					this.doCompute(trans, index);

			        this.dealSuccess(trans, list);    
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					
					dealException(e, trans, list);
            break;
				}
    
			}
		
		}
		return list;
	}

	/**
	 * 统一异常处理方法
	 * @param e
	 * @return
	 */
	protected void dealException(Exception e, ServiceTransaction trans, List<TaskResult> list) {
		
		/*
		 * 异常
		 * 		直接回滚
		 *      返回异常对象
		 */
		TaskResult result = new TaskResult();
		result.trans = null;
		try {
			trans.notifyRollback();
		} catch (BaseException e1) {
			// TODO Auto-generated catch block
			result.rollbackFail = new Boolean(true);
			result.be = e1; //如果回滚失败，返回回滚的异常
			list.add(result);
			return ;
		}finally{
			//清除任务失败的事务对象
			ServiceCommonUtil.clearTransactionCache(trans.getTransId());
		}
		result.rollbackFail = new Boolean(false);
		result.be = e;
		list.add(result);
		return ;
	}
	
	/**
	 * 统一成功处理方法
	 * @param trans
	 * @param list
	 */
	protected void dealSuccess(ServiceTransaction trans, List<TaskResult> list) {
		 TaskResult result = new TaskResult();
	     result.rollbackFail = new Boolean(false);
	     result.trans = trans;
	     result.be = null; 
	     list.add(result);
	}
	
	/**
	 * 具体的任务操作
	 * @param trans
	 */
	protected abstract void doCompute(ServiceTransaction trans, int index) throws BaseException;
	
	/**
	 *  拆分较大的子任务
	 * @param start
	 * @param mid
	 * @param end
	 * @return
	 */
	protected abstract List<? extends VirtualPortTask> splitTasks(int start, int mid, int end);
}
