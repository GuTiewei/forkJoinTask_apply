/**
 * 任务执行器
 *    
 * @author gWX378859
 *
 */
public class TaskExecutor {

	private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);
	
	private static ForkJoinPool pool = new ForkJoinPool();
	
	/**
	 * 执行任务
	 * @param border
	 * @param pe
	 * @param preTransList 上一次任务执行成功的事务对象
	 * @return
	 * @throws BaseException
	 */
	public static List<ServiceTransaction> execute(VirtualTask border, VirtualTask pe, List<ServiceTransaction>  preTransList) throws Exception{
		
		//提交两次任务
		List<TaskResult> results = new ArrayList<>();
		ForkJoinTask<List<TaskResult>> borderResult = null;
		ForkJoinTask<List<TaskResult>> peResult = null;
		if(null != border)
			borderResult = pool.submit(border);
		if(null != pe)
			peResult = pool.submit(pe);
		//等待结果
		if(null != borderResult)
			results.addAll(borderResult.join());
		if(null != peResult)
			results.addAll(peResult.join());
		
		return dealReault(results, preTransList);
	}
	
	
	/**
	 * 任务结果处理
	 * @param results
	 * @param preTransList
	 * @return
	 * @throws BaseException
	 */
	private static List<ServiceTransaction> dealReault(List<TaskResult> results, List<ServiceTransaction>  preTransList) throws Exception {

		logger.info("task results size:" + results.size());
		boolean needRollback = false;
		Exception ex = null;
		for(TaskResult result : results) {
			if(result.be != null) { 
				needRollback = true;  //如果任务有异常返回，需要回滚
				/*
				 * 取最后一次异常
				 */
				ex = result.be;
				if(result.rollbackFail) {
					logger.error("fork task rollback fail.", result.be);
				}
			}		
		}
		
		List<ServiceTransaction> transRetrun = new ArrayList<>();
		if(needRollback) {
			logger.info("start rollback");
			
			/*
			 * 本次task执行成功的事务回滚
			 */
			for(TaskResult result : results) {
				if(null != result.trans) {
					try {
						result.trans.notifyRollback();
					} catch (BaseException e) {
						// TODO Auto-generated catch block
						ex = e;
						logger.error("join task rollback fail.",e);						
					}finally{
						ServiceCommonUtil.clearTransactionCache(result.trans.getTransId());
					}
				}
			}
			
			/*
			 * 上一次task执行成功的事务回滚
			 */
			if(null != preTransList && preTransList.size() > 0) {
				for(int i = preTransList.size() - 1; i >= 0; i--) {
					try{
						preTransList.get(i).notifyRollback();
					}catch (BaseException e) {
						// TODO Auto-generated catch block
						ex = e;
						logger.error("pre task rollback fail.",e);
					}finally{
						ServiceCommonUtil.clearTransactionCache(preTransList.get(i).getTransId());
					}
				}

			}
			//清除上次的所有事务
			if(null != preTransList && preTransList.size() > 0)
				preTransList.clear();
		
			/*
			 * 抛出最后一次的回滚异常
			 */
			if(ex != null){
				throw ex;
			}
			
		} else {
			/*
			 * 如果执行成功返回所有的事务对象
			 */
			if(null != preTransList)
				transRetrun.addAll(preTransList);
			for(TaskResult result : results) {
				if(null != result.trans) {
					transRetrun.add(result.trans);
				}
			}
		}
		//pool.shutdown();
		
		return transRetrun;
	}

	/**
	 * 
	 * @param border
	 * @param pe
	 * @param parent 事务结点
	 * @return
	 * @throws TaskException
	 */
	public static ServiceTransaction execute(VirtualPortTask border, VirtualPortTask pe, ServiceTransaction  parent) throws TaskException{
		
		//提交两次任务
		List<TaskResult> results = new ArrayList<>();
		ForkJoinTask<List<TaskResult>> borderResult = null;
		ForkJoinTask<List<TaskResult>> peResult = null;
		if(null != border)
			borderResult = pool.submit(border);
		if(null != pe)
			peResult = pool.submit(pe);
		//等待结果
		if(null != borderResult)
			results.addAll(borderResult.join());
		if(null != peResult)
			results.addAll(peResult.join());
		
		return dealReault(results, parent);
	}

	/**
	 * 执行任务
	 * @param tasks
	 * @param parent 事务结点
	 * @return
	 * @throws TaskException
	 */
	public static ServiceTransaction execute(List<VirtualPortTask> tasks, ServiceTransaction parent) throws TaskException {
		List<TaskResult> results = new ArrayList<>();
		List<ForkJoinTask<List<TaskResult>>> taskResults = null;
		
		if(null != tasks) {
			taskResults = new ArrayList<>();
			for(VirtualPortTask task : tasks) {
				taskResults.add( pool.submit(task));
			}
		}
		if(null != taskResults) {
			for(ForkJoinTask<List<TaskResult>> tr : taskResults) {
				results.addAll(tr.join());
			}
		}
		return dealReault(results, parent);
	}
	/**
	 * 任务结果处理
	 * @param results
	 * @param parent
	 * @return
	 * @throws TaskException
	 */
	private static ServiceTransaction dealReault(List<TaskResult> results, ServiceTransaction parent) throws TaskException {

		logger.info("task results size:" + results.size());
		boolean hasException = false;
		
		List<ServiceTransaction> rollbackFailTrans = new ArrayList<>();
		List<ServiceTransaction> successTrans = new ArrayList<>();
		TaskException te = null;
		for(TaskResult result : results) {
			if(result.be != null) { 
				hasException = true;  //如果任务有异常返回，需要回滚
				/*
				 * 取最后一次异常
				 */
				
				if(result.rollbackFail) {
					te = new TaskException(true, ErrorConstants.TASK_VIRTUAL_PORT, result.be);
					logger.error("fork task rollback fail.");
					rollbackFailTrans.add(result.trans);	
				}
				else if(te == null) {
					/*
					 * 返回的异常中包含执行失败的异常和回滚失败的异常
					 * 因为执行失败的异常不能覆盖回滚失败的异常
					 * 所以只有当te == null 时，才可以赋值
					 */
					te = new TaskException(false, ErrorConstants.TASK_VIRTUAL_PORT, result.be);
				}
				
			} else
				successTrans.add(result.trans);
			
		}
		if(hasException) {
			/*
			 * 本次task执行成功的事务回滚
			 */
			if(!successTrans.isEmpty()) {
				logger.info("start rollback");
				RollbackTask rollbackTask = new RollbackTask(successTrans, 0, successTrans.size());
				ForkJoinTask<List<TaskResult>> rollback = pool.submit(rollbackTask);
				List<TaskResult> rollbackResults = rollback.join();
				
				for(TaskResult result : rollbackResults) {
					//回滚失败的事务加入列表
					if(result.be != null) {
						logger.error("fork rollback task fail.");
						rollbackFailTrans.add(result.trans);
						te = new TaskException(true, ErrorConstants.TASK_VIRTUAL_PORT, result.be);
					} 
				}
			}
			
			if(!rollbackFailTrans.isEmpty()) {
				logger.info("add childList:" + rollbackFailTrans.size());
				parent.addChildList(rollbackFailTrans);
			}

			/*
			 * 抛出最后一次的异常
			 */
			throw te;	
		} else {
			logger.info("fork join tasks success.");
			/*
			 * 如果执行成功，将所有事务加入parent
			 */
			logger.info("add childList:" + successTrans.size());
			parent.addChildList(successTrans);
		}
		//pool.shutdown();
		/*
		 * 如果成功，返回第一个成功的事务，作为下一阶段任务的parent
		 * 否则返回parent
		 */
		if(successTrans.size() > 0)
			return successTrans.get(0);
		else 
			return parent;
	}
}
