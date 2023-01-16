package entity;

import java.io.Serializable;
import java.net.Socket;
/**
 * Created by Administrator on 2016/6/27.
 * 计算节点类,是client和server的父类，主要包括ip地址
 * 端口号，同时声明了抽象读写网络流中的方法。
 */
public abstract class Node
{
    /**
     * 节点名称
     */
    private java.lang.String name;
    /**
     * 节点地址
     */
    private java.lang.String ipAddr;
    private int port;
    public int getPort()
    {
        return port;
    }
    public void setPort(int port)
    {
        this.port = port;
    }
    public java.lang.String getName()
    {
        return name;
    }
    public void setName(java.lang.String name)
    {
        this.name = name;
    }
    public java.lang.String getIpAddr()
    {
        return ipAddr;
    }
    public void setIpAddr(java.lang.String ipAddr)
    {
        this.ipAddr = ipAddr;
    }
    public abstract void readData(Socket socket);
    /**
     * 向网络中写入消息
     * @param socket
     * @param writeData
     */
    public abstract void writeData(final Socket socket, final Message... writeData);
}
