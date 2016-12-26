/**
 * 回滚事务处理类

 */
public class ServiceTransaction implements IServiceTransaction,Serializable {
	  /**
     * 注释内容
     */
    private static final long serialVersionUID = -5026878831203201738L;

    private static final Logger logger = LoggerFactory.getLogger(ServiceTransaction.class);

    private final List<ICommonServiceObserver> observerList = new ArrayList<ICommonServiceObserver>();

    private ServiceTransaction parent = null;
    
    private List<ServiceTransaction> children = new LinkedList<>();
    
    private String msgId;
	
	private String transId;

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId){
        this.msgId = msgId;
    }

    public String getTransId() {
        return transId;
    }

    public void setTransId(String transId){
        this.transId = transId;
    }

    public int getObserverListSize(){
        return observerList.size();
    }

    @Override
    public void add(ICommonServiceObserver observer){
    	if(observer != null)
    		logger.info("add observer:" + observer.getClass().getName());
        observerList.add(observer);
      //更新数据库
        this.saveOrUpdateTran();
    }

    @Override
    public void remove(ICommonServiceObserver observer){
    	if(observer != null)
    		logger.info("remove observer:" + observer.getClass().getName());
        observerList.remove(observer);
      //更新数据库
        this.removeOrUpdateTran();
    }

    @Override
    public void notifyRollback() throws BaseException{
        logger.info("Begin to rollback transaction.");
        boolean rollbackSuccess = true;
        if(this.hasChild()) {
        	/*
        	 * 同一级别的子事务回滚
        	 */
        	List<ServiceTransaction> copy = new ArrayList<>(this.children);
        	for(ServiceTransaction child : copy) {	
        		try{
        			child.notifyRollback();
        			//成功回滚后删除
        			this.removeChild(child);
        		} catch(Exception be) {
        			rollbackSuccess = false;
        			logger.error(be.getMessage(), be);
        			break;
        		}
        	}
        }

        //子事务回滚成功，回滚当前事务
        if(!this.hasChild()) {
        	/*
        	 * 当前事务回滚时，只要一个observer回滚失败，立即停止回滚
        	 */
        	int size = observerList.size();
            for (int i = size - 1; i >= 0; i--) {
                try{
                	ICommonServiceObserver observer = observerList.get(i);
                	observer.rollback();
                	this.remove(observer);
                }
                catch(Exception e){
                    logger.info(String.valueOf(observerList.get(i))+",");
                    logger.error(e.getMessage(), e);
                    rollbackSuccess = false;
                    // remove or update 
                    break;
                }
            }  
            
        }
        if(!rollbackSuccess)
        	throw new ValidatorException(ErrorConstants.ROLLCABK_ERROR);
        logger.info("Success in rollbacking transaction.");
    }
    
    private void saveOrUpdateTran(){
    	//获取root事务对象
    	ServiceTransaction rootTrans = this.getRoot();
		TransactionEntity entity = TransactionDao.getInstance().queryTransaction(UUID.fromString(rootTrans.getTransId()));
        logger.error("==={}",rootTrans.getObserverListSize());
        if (null == entity) {
            entity = new TransactionEntity();
            entity.setMessageID(rootTrans.getMsgId());
            entity.setTransactionID(UUID.fromString(rootTrans.getTransId()));
            entity.setTransaction(Base64.encodeBase64String(SerializeUtil.objectToByteArray(rootTrans)));
            logger.error("-------{} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTime());
            logger.info("save trans:" + rootTrans.getTransId());
            TransactionDao.getInstance().saveTransaction(entity);
        }
        else {
            entity.setTransaction(Base64.encodeBase64String(SerializeUtil.objectToByteArray(rootTrans)));
            logger.error("-------{} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTime());
            logger.info("update trans:" + rootTrans.getTransId());
            TransactionDao.getInstance().updateTransaction(entity);
        }
        
       
        
    }
    
  
    private void removeOrUpdateTran() {
    	//获取root事务对象
    	ServiceTransaction rootTrans = this.getRoot();
        
        TransactionEntity entity = TransactionDao.getInstance().queryTransaction(UUID.fromString(rootTrans.getTransId()));

        if (null != entity){
            logger.error("==={}",rootTrans.getObserverListSize());
            logger.error("========{} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTime());
            if(rootTrans.getObserverListSize() == 0 && !rootTrans.hasChild()){
                logger.info("delete trans:" + rootTrans.getTransId());
                TransactionDao.getInstance().deleteTransaction(entity.getTransactionID());
            }else{
                logger.info("update trans:" + rootTrans.getTransId());
                entity.setTransaction(Base64.encodeBase64String(SerializeUtil.objectToByteArray(rootTrans)));
                TransactionDao.getInstance().updateTransaction(entity);
            }
        }
    }
    
    private void setParent(ServiceTransaction parent) {
		// TODO Auto-generated method stub
		this.parent = parent;
	}
	
	private ServiceTransaction removeParent() {
		// TODO Auto-generated method stub
		ServiceTransaction old = this.parent;
		this.parent = null;
		return old;
	}

	public void addChild(ServiceTransaction child) {
		// TODO Auto-generated method stub
		if(null != child) {
			logger.info("add child:" + child.getClass().getName());
			if(this.children == null)
				this.children = new LinkedList<>();
			this.children.add(child);
			//将child从原先的事务树脱离
			child.separateFromTransTree();
			child.setParent(this);
			//更新数据库
			this.saveOrUpdateTran();
		}
	}
	
	
	public void addChildList(List<ServiceTransaction> childList) {
		if(null != children && !childList.isEmpty()) {
			if(this.children == null)
				this.children = new LinkedList<>();
			this.children.addAll(childList);
			for(ServiceTransaction child : childList) {
				logger.info("add child:" + child.getClass().getName());
				child.separateFromTransTree();
				child.setParent(this);
			}
			//更新数据
			this.saveOrUpdateTran();
		}
	}

	public void removeChild(ServiceTransaction child) {
		// TODO Auto-generated method stub
		if(null != child && this.children != null && this.children.contains(child)) {
			logger.info("remove child:" + child.getClass().getName());
			this.children.remove(child);
			child.removeParent();
			//更新数据库
			this.removeOrUpdateTran();
		}
	}

	public boolean hasChild() {
		// TODO Auto-generated method stub
		return this.children != null && this.children.size() > 0;
	}


	public ServiceTransaction getRoot() {
		// TODO Auto-generated method stub
		if(this.parent == null)
			return this;
		else
			return this.parent.getRoot();
	}
	
	/**
	 * 删除当前事务的transEntity
	 * 一般在parent切换的时候调用
	 */
	private void separateFromTransTree() {

			if(this.transId != null) {
			    TransactionEntity entity = TransactionDao.getInstance().queryTransaction(UUID.fromString(this.getTransId()));
			         
		        if (null != entity{
		            logger.error("========{} {} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTransaction(),entity.getTime());
		            logger.info("delete trans:" + this.getTransId());
		            logger.error("==={}",this.getObserverListSize());
		            TransactionDao.getInstance().deleteTransaction(entity.getTransactionID());
		        }
			}     
			//丢弃该事务
	        if(this.parent != null)
	           this.parent.removeChild(this);     
	}
	
}/**
 * 回滚事务处理类

 */
public class ServiceTransaction implements IServiceTransaction,Serializable {
	  /**
     * 注释内容
     */
    private static final long serialVersionUID = -5026878831203201738L;

    private static final Logger logger = LoggerFactory.getLogger(ServiceTransaction.class);

    private final List<ICommonServiceObserver> observerList = new ArrayList<ICommonServiceObserver>();

    private ServiceTransaction parent = null;
    
    private List<ServiceTransaction> children = new LinkedList<>();
    
    private String msgId;
	
	private String transId;

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId){
        this.msgId = msgId;
    }

    public String getTransId() {
        return transId;
    }

    public void setTransId(String transId){
        this.transId = transId;
    }

    public int getObserverListSize(){
        return observerList.size();
    }

    @Override
    public void add(ICommonServiceObserver observer){
    	if(observer != null)
    		logger.info("add observer:" + observer.getClass().getName());
        observerList.add(observer);
      //更新数据库
        this.saveOrUpdateTran();
    }

    @Override
    public void remove(ICommonServiceObserver observer){
    	if(observer != null)
    		logger.info("remove observer:" + observer.getClass().getName());
        observerList.remove(observer);
      //更新数据库
        this.removeOrUpdateTran();
    }

    @Override
    public void notifyRollback() throws BaseException{
        logger.info("Begin to rollback transaction.");
        boolean rollbackSuccess = true;
        if(this.hasChild()) {
        	/*
        	 * 同一级别的子事务回滚
        	 */
        	List<ServiceTransaction> copy = new ArrayList<>(this.children);
        	for(ServiceTransaction child : copy) {	
        		try{
        			child.notifyRollback();
        			//成功回滚后删除
        			this.removeChild(child);
        		} catch(Exception be) {
        			rollbackSuccess = false;
        			logger.error(be.getMessage(), be);
        			break;
        		}
        	}
        }

        //子事务回滚成功，回滚当前事务
        if(!this.hasChild()) {
        	/*
        	 * 当前事务回滚时，只要一个observer回滚失败，立即停止回滚
        	 */
        	int size = observerList.size();
            for (int i = size - 1; i >= 0; i--) {
                try{
                	ICommonServiceObserver observer = observerList.get(i);
                	observer.rollback();
                	this.remove(observer);
                }
                catch(Exception e){
                    logger.info(String.valueOf(observerList.get(i))+",");
                    logger.error(e.getMessage(), e);
                    rollbackSuccess = false;
                    // remove or update 
                    break;
                }
            }  
            
        }
        if(!rollbackSuccess)
        	throw new ValidatorException(ErrorConstants.ROLLCABK_ERROR);
        logger.info("Success in rollbacking transaction.");
    }
    
    private void saveOrUpdateTran(){
    	//获取root事务对象
    	ServiceTransaction rootTrans = this.getRoot();
		TransactionEntity entity = TransactionDao.getInstance().queryTransaction(UUID.fromString(rootTrans.getTransId()));
        logger.error("==={}",rootTrans.getObserverListSize());
        if (null == entity) {
            entity = new TransactionEntity();
            entity.setMessageID(rootTrans.getMsgId());
            entity.setTransactionID(UUID.fromString(rootTrans.getTransId()));
            entity.setTransaction(Base64.encodeBase64String(SerializeUtil.objectToByteArray(rootTrans)));
            logger.error("-------{} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTime());
            logger.info("save trans:" + rootTrans.getTransId());
            TransactionDao.getInstance().saveTransaction(entity);
        }
        else {
            entity.setTransaction(Base64.encodeBase64String(SerializeUtil.objectToByteArray(rootTrans)));
            logger.error("-------{} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTime());
            logger.info("update trans:" + rootTrans.getTransId());
            TransactionDao.getInstance().updateTransaction(entity);
        }
        
       
        
    }
    
  
    private void removeOrUpdateTran() {
    	//获取root事务对象
    	ServiceTransaction rootTrans = this.getRoot();
        
        TransactionEntity entity = TransactionDao.getInstance().queryTransaction(UUID.fromString(rootTrans.getTransId()));

        if (null != entity){
            logger.error("==={}",rootTrans.getObserverListSize());
            logger.error("========{} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTime());
            if(rootTrans.getObserverListSize() == 0 && !rootTrans.hasChild()){
                logger.info("delete trans:" + rootTrans.getTransId());
                TransactionDao.getInstance().deleteTransaction(entity.getTransactionID());
            }else{
                logger.info("update trans:" + rootTrans.getTransId());
                entity.setTransaction(Base64.encodeBase64String(SerializeUtil.objectToByteArray(rootTrans)));
                TransactionDao.getInstance().updateTransaction(entity);
            }
        }
    }
    
    private void setParent(ServiceTransaction parent) {
		// TODO Auto-generated method stub
		this.parent = parent;
	}
	
	private ServiceTransaction removeParent() {
		// TODO Auto-generated method stub
		ServiceTransaction old = this.parent;
		this.parent = null;
		return old;
	}

	public void addChild(ServiceTransaction child) {
		// TODO Auto-generated method stub
		if(null != child) {
			logger.info("add child:" + child.getClass().getName());
			if(this.children == null)
				this.children = new LinkedList<>();
			this.children.add(child);
			//将child从原先的事务树脱离
			child.separateFromTransTree();
			child.setParent(this);
			//更新数据库
			this.saveOrUpdateTran();
		}
	}
	
	
	public void addChildList(List<ServiceTransaction> childList) {
		if(null != children && !childList.isEmpty()) {
			if(this.children == null)
				this.children = new LinkedList<>();
			this.children.addAll(childList);
			for(ServiceTransaction child : childList) {
				logger.info("add child:" + child.getClass().getName());
				child.separateFromTransTree();
				child.setParent(this);
			}
			//更新数据
			this.saveOrUpdateTran();
		}
	}

	public void removeChild(ServiceTransaction child) {
		// TODO Auto-generated method stub
		if(null != child && this.children != null && this.children.contains(child)) {
			logger.info("remove child:" + child.getClass().getName());
			this.children.remove(child);
			child.removeParent();
			//更新数据库
			this.removeOrUpdateTran();
		}
	}

	public boolean hasChild() {
		// TODO Auto-generated method stub
		return this.children != null && this.children.size() > 0;
	}


	public ServiceTransaction getRoot() {
		// TODO Auto-generated method stub
		if(this.parent == null)
			return this;
		else
			return this.parent.getRoot();
	}
	
	/**
	 * 删除当前事务的transEntity
	 * 一般在parent切换的时候调用
	 */
	private void separateFromTransTree() {

			if(this.transId != null) {
			    TransactionEntity entity = TransactionDao.getInstance().queryTransaction(UUID.fromString(this.getTransId()));
			         
		        if (null != entity{
		            logger.error("========{} {} {} {}", entity.getMessageID(),entity.getTransactionID(),entity.getTransaction(),entity.getTime());
		            logger.info("delete trans:" + this.getTransId());
		            logger.error("==={}",this.getObserverListSize());
		            TransactionDao.getInstance().deleteTransaction(entity.getTransactionID());
		        }
			}     
			//丢弃该事务
	        if(this.parent != null)
	           this.parent.removeChild(this);     
	}
	
}
