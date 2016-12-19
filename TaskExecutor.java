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
}
