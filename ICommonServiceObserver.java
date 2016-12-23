/**
 * 原子级别操作的回滚接口，用于事务失败时的回滚。
 */
public interface ICommonServiceObserver extends Serializable
{

    /**
     * 注释内容
     */
    static final long serialVersionUID = 1L;

    /**
     * 回滚
  
     */
    public void rollback() throws BaseException;


}
