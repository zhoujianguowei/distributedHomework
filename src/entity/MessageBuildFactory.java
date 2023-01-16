package entity;

import java.io.Serializable;
/**
 * Created by Administrator on 2016/6/28.
 */
public class MessageBuildFactory
{
    /**
     * server广播client可以开始工作
     */
    public final static String SERVER_START = "start";

    public final static String SERVER_DETECT = "detect";
    /**
     * client向server请求最新的pagerank值
     */
    public final static String CLIENT_REQUEST = "get_pagerank";
    public final static String SERVER_RESPONSE = "response_pagerank";
    /**
     * 正常迭代命令
     */
    public final static String SERVER_NORMAL_ITERATE = "normal_iterate";
    /**
     * client向server提交本次计算的结果
     */
    public final static String CLIENT_COMMIT = "commit";
    /**
     * server通知所有client计算完成
     */
    public final static String SERVER_TASK_OVER = "task_over";
    /**
     * 构建Message
     * @param command 消息类型
     * @param tag   消息源，消息是由服务端或者是客户端发出
     * @param srcNodeAddr   消息源主机地址
     * @param desNodeAddr   消息目的主机地址
     * @param content       消息内容，可选
     * @return
     */
    public static synchronized Message reconstructMessage(
            String command, int tag, String srcNodeAddr, String desNodeAddr, Serializable... content
    )
    {
        Message msg = null;
        switch (command)
        {
            case SERVER_START:
                msg = Message.restructMsg(Message.SERVER_REQUEST_START_WORK_TYPE, tag, srcNodeAddr, desNodeAddr,
                        content[0]);
                break;
            case SERVER_DETECT:
                msg = Message.restructMsg(Message.SERVER_REQUEST_ALIVE_TYPE, tag, srcNodeAddr, desNodeAddr);
                break;
            case CLIENT_REQUEST:
                msg = Message.restructMsg(Message.CLIENT_REQUEST_PG_TYPE, tag, srcNodeAddr, desNodeAddr);
                break;
            case SERVER_RESPONSE:
                msg = Message.restructMsg(Message.SERVER_RESPONSE_PG_TYPE, tag, srcNodeAddr, desNodeAddr, content[0]);
                break;
            case SERVER_NORMAL_ITERATE:
                msg = Message.restructMsg(Message.SERVER_REQUEST_NORMAL_ITERATE, tag, srcNodeAddr, desNodeAddr);
                break;
            case CLIENT_COMMIT:
                msg = Message.restructMsg(Message.CLIENT_REQUEST_COMMIT_PG_TYPE, tag, srcNodeAddr, desNodeAddr,
                        content[0]);
                break;
            case SERVER_TASK_OVER:
                //content[0]包含最终的结果
                msg = Message.restructMsg(Message.SERVER_REQUEST_TASK_OVER_TYPE, tag, srcNodeAddr, desNodeAddr, content[0]);
                break;


        }
        return msg;
    }
}
