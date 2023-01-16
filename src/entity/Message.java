package entity;

import java.io.Serializable;

import static javafx.scene.input.KeyCode.L;

/**
 * Created by Administrator on 2016/6/27.
 * 节点之间传递的消息
 */
public class Message implements Serializable
{
    private static final long serialVersionUID = 35378572531234199L;
    private int type; //消息类型
    private String srcNodeAddr;
    private String desNodeAddr;
    private int tag; //tag=0代表服务端（master）发送的，1 client
    private Serializable content;
    public static final int SERVER_TAG = 0;
    public static final int CLIENT_TAG = 1;
    public static final int CLIENT_REQUEST_PG_TYPE = 3;//向server请求最新的pageRank
    public static final int CLIENT_REQUEST_COMMIT_PG_TYPE = 4;//向server提交本地local结果
    public static final int SERVER_REQUEST_START_WORK_TYPE = 5;//向client发送开始计算命令，同时发送对应的部分图
    public static final int SERVER_REQUEST_NORMAL_ITERATE = 6;//向client发送计算superstep
    public static final int SERVER_RESPONSE_PG_TYPE = 7;//向client发送最新的pageRank值
    public static final int SERVER_REQUEST_ALIVE_TYPE = 8;//询问client的状态
    public static final int SERVER_REQUEST_TASK_OVER_TYPE =9;//同时client任务已经完成
    public static final int SERVER_REQUEST_OTHER_TYPE = 10; //其它状态

    public Message(int type)
    {
        this.type = type;
    }

    public int getType()
    {
        return type;
    }

    public void setType(int type)
    {
        this.type = type;
    }

    public String getSrcNodeAddr()
    {
        return srcNodeAddr;
    }

    public void setSrcNodeAddr(String srcNodeAddr)
    {
        this.srcNodeAddr = srcNodeAddr;
    }

    public String getDesNodeAddr()
    {
        return desNodeAddr;
    }

    public void setDesNodeAddr(String desNodeAddr)
    {
        this.desNodeAddr = desNodeAddr;
    }

    public int getTag()
    {
        return tag;
    }

    public void setTag(int tag)
    {
        this.tag = tag;
    }

    public Serializable getContent()
    {
        return content;
    }

    public void setContent(Serializable content)
    {
        this.content = content;
    }

    public static synchronized Message restructMsg(
            int msgType, int tag, String srcNodeAddr, String desNodeAddr, Serializable... content
    )
    {
        Message msg = new Message(msgType);
        msg.setSrcNodeAddr(srcNodeAddr);
        msg.setDesNodeAddr(desNodeAddr);
        msg.setType(msgType);
        if (content.length > 0)
        {
            msg.setContent(content[0]);
        }
        return msg;
    }

    @Override
    public String toString()
    {
        return "Message{" +
                "type=" + type +
                ", srcNodeAddr='" + srcNodeAddr + '\'' +
                ", desNodeAddr='" + desNodeAddr + '\'' +
                ", tag=" + tag +
                ", content=" + content +
                '}';
    }
}
