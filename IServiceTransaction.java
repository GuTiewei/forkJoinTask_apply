/**
 * 数据库事务处理接口

 */
public interface IServiceTransaction extends ICommonServiceTransaction
{
    /**
     * 增加原子操作
     * @param observer 原子操作对象
     */
    @Override
    public void add(ICommonServiceObserver observer);

    /**
     * 删除原子操作
     * @param observer 原子操作对象
     */
    @Override
    public void remove(ICommonServiceObserver observer);

    /**
     * 通知事务中的每个原子回滚
     * @throws BaseException <br>
     */
    @Override
    public void notifyRollback() throws BaseException;

}
