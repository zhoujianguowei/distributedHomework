package entity;

import utils.Constant;
import utils.PageGraphUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Created by Administrator on 2016/6/28.
 */
public class Server extends Node
{
    /**
     * 保存客户端连接绑定的socket列表
     */
    ArrayList<Socket> clientSocketList;
    /**
     * 对应client端的输入流
     */
    HashMap<Socket, ObjectInputStream> clientObjInMap;
    HashMap<Socket, ObjectOutputStream> clientObjOutMap;
    /**
     * server端所允许连接client数量
     */
    int backlog = 10;
    /**
     * 用来标记是否开始计算
     */
    volatile boolean isWorkStarted = false;
    volatile boolean isDetectClientsAliveThreadStart = false;//检测client状态线程
    volatile boolean isBroadcastThreadStart = false;
    /**
     * 负责计算同步
     */
    AtomicInteger clientCyclicBarrier;
    /**
     * 计算的pagerank值写入文件父目录
     */
    File pageRankFileParent;
    int pageRankSuperstep = 0;
    /**
     * 之前superstep的pagerank
     */
    HashMap<String, Double> previousPgMap;
    /**
     * 本地计算合并使用
     */
    HashMap<String, Double> updatedPgMap;
    private int pageRankVersion = 0;
    /**
     * 宕机的client的地址
     */
    HashSet<String> lostClientsAddrSet;
    /**
     * pagerank中的随机跳转系数
     */
    private static final double d = 0.15;
    private Object clientSocketOperationMonitor = new Object();
    /**
     * 用来记录client的最大数量
     */
    private int clientsCount = 0;
    public Server()
    {
        clientSocketList = new ArrayList<>();
        clientObjInMap = new HashMap<>();
        clientObjOutMap = new HashMap<>();
        clientCyclicBarrier = new AtomicInteger();
        pageRankFileParent = new File("." + File.separator + "src");
        previousPgMap = new HashMap<>();
        updatedPgMap = new HashMap<>();
        lostClientsAddrSet = new HashSet<>();
        try
        {
            initParams();
        }
        catch (IOException e1)
        {
            System.out.println("客户端连接出错+" + e1.getMessage());
        }
    }
    /**
     * server端等待client连接，并做一些初始化工作
     *
     * @throws IOException
     */
    private void initParams() throws IOException
    {
        ServerSocket serverSocket = new ServerSocket(Constant.SERVER_PORT, backlog);
        while (true)
        {
            final Socket clientSocket = serverSocket.accept();
            String ipAddr = Constant.getIpAddr(clientSocket);
            if (isWorkStarted && !lostClientsAddrSet.contains(ipAddr))
            {
                System.out.println("已经开始工作，拒绝连接");
                continue;
            }
            if (!lostClientsAddrSet.contains(ipAddr))
            {
                System.out.println(ipAddr + "客户端连接");
                clientSocketList.add(clientSocket);
                clientObjOutMap.put(clientSocket, new ObjectOutputStream(clientSocket.getOutputStream()));
                clientObjInMap.put(clientSocket, new ObjectInputStream(clientSocket.getInputStream()));
            }
            else
            {
                System.err.println(ipAddr + "客户端重新连接");
                //删除对应的socket
                lostClientsAddrSet.remove(ipAddr);
                clientSocketList.add(clientSocket);
                //注意二者声明的顺序,该题强调server先发送消息
                clientObjOutMap.put(clientSocket, new ObjectOutputStream(clientSocket.getOutputStream()));
                clientObjInMap.put(clientSocket, new ObjectInputStream(clientSocket.getInputStream()));
                //通知该消亡的client继续工作
                notifyNormalIterateSuperstepWork(clientSocket, previousPgMap);
            }
            new Thread()
            {
                @Override
                public void run()
                {
                    //                        super.run();
                    readData(clientSocket);
                }
            }.start();
            new Thread()
            {
                @Override
                public void run()
                {
                    //                        super.run();
                    //                        writeData(clientSocket);
                    broadcast();
                }
            }.start();
        }
    }
    /**
     * server获得client消息后，做对应的处理
     *
     * @param socket server端负责与client通讯的端口
     * @param recMsg 接受的消息
     */
    private void receiveData(Socket socket, Message recMsg)
    {
        Message sendMsg = null;
        //        System.out.println("server receive:" + recMsg.getType() + " from " + Constant.getIpAddr(socket));
        switch (recMsg.getType())
        {
            case Message.CLIENT_REQUEST_PG_TYPE:
                HashMap<String, Object> wrapMap = new HashMap<>();
                wrapMap.put(Constant.PAGE_RANK_SUPERSTEP_KEY, pageRankVersion);
                wrapMap.put(Constant.CONTENT_KEY, previousPgMap);
                sendMsg = MessageBuildFactory
                        .reconstructMessage(MessageBuildFactory.SERVER_RESPONSE, Message.SERVER_TAG,
                                Constant.SERVER_HOST, Constant.getIpAddr(socket), wrapMap);
                writeData(socket, sendMsg);
                break;
            case Message.CLIENT_REQUEST_COMMIT_PG_TYPE:
                mergeResult(updatedPgMap, (HashMap<String, Double>) recMsg.getContent());
                int value = clientCyclicBarrier.decrementAndGet();
                if (value == 0)
                {
                    Iterator<String> vertextKeyIterator = updatedPgMap.keySet().iterator();
                    synchronized (this)
                    {
                        double deadEndsPg = getDeadEndsPg(PageGraphUtils.getSingleInstance().getVertexHashMap(),
                                previousPgMap);
                        while (vertextKeyIterator.hasNext())
                        {
                            String key = vertextKeyIterator.next();
                            double inEdgePg = updatedPgMap.get(key);

                            /*
                            ****************
                             */
                            //                            updatedPgMap.put(key, (deadEndsPg + inEdgePg) * (1 - d) +
                            // d);这句话好像有问题
                            updatedPgMap.put(key, inEdgePg * (1 - d) + d / updatedPgMap.size());
                        }
                        double distance = computeDistance(updatedPgMap, previousPgMap);
                        pageRankVersion++;
                        System.out.println("第" + pageRankVersion + "步superstep");
                        if (distance < Constant.CONVERGENCE_DIFF)
                        {
                            System.out.println("计算完成了");
                            notifyClientsJobOver(updatedPgMap);
                            writeFinalResult(updatedPgMap);
                            //                            System.out.println(updatedPgMap);
                        }
                        else
                        {
                            clientCyclicBarrier.set(clientsCount);
                            previousPgMap = new HashMap<>(updatedPgMap);
                            notifyNormalIterateSuperstepWork(clientSocketList, previousPgMap);
                            clearUpdatedPgMapAfterIterate();
                        }
                        //注意放置位置，以及参数
                        writeSuperstepPagerankToDisk(previousPgMap, pageRankVersion);
                    }
                }
                break;
        }
    }
    /**
     * 按照pageRank的值从小到大排序，并写入到文件中
     */
    private void writeFinalResult(HashMap<String, Double> pgMap)
    {
        HashMap<String, Vertex> vertexGraph = PageGraphUtils.getSingleInstance().getVertexHashMap();
        ArrayList<String> keyList = new ArrayList<>();
        ArrayList<Double> pgList = new ArrayList<>();
        ArrayList<Integer> inEdgesList = new ArrayList<>();
        Iterator<String> pgMapIterator = pgMap.keySet().iterator();
        while (pgMapIterator.hasNext())
        {
            String key = pgMapIterator.next();
            keyList.add(key);
            pgList.add(pgMap.get(key));
        }
        for (int i = 0; i < pgList.size(); i++)
        {
            int maxPgIndex = i;
            for (int j = i; j < pgList.size(); j++)
            {
                if (pgList.get(maxPgIndex) < pgList.get(j))
                {
                    maxPgIndex = j;
                }
            }
            double temp = pgList.get(i);
            pgList.set(i, pgList.get(maxPgIndex));
            pgList.set(maxPgIndex, temp);
            String keyTemp = keyList.get(i);
            keyList.set(i, keyList.get(maxPgIndex));
            keyList.set(maxPgIndex, keyTemp);
        }
        try
        {
            BufferedWriter writer =
                    new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("result.txt"))));
            writer.write("#NODE\tPGRANK\tINEDGE\r\n");
            for (int i = 0; i < keyList.size(); i++)
            {
                String key = keyList.get(i);
                String writeLine = key + "\t" + pgList.get(i) + "\t" + vertexGraph.get(key).getInEdge() + "\r\n";
                writer.write(writeLine);
            }
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    /**
     * 获取只有入度没有出度的client所能贡献的pagerank值
     *
     * @param vertexHashMap
     * @param pgMap
     * @return
     */
    private double getDeadEndsPg(HashMap<String, Vertex> vertexHashMap, HashMap<String, Double> pgMap)
    {
        double deadEndsValue = 0;
        Iterator<String> vertexKeyIterator = vertexHashMap.keySet().iterator();
        while (vertexKeyIterator.hasNext())
        {
            String key = vertexKeyIterator.next();
            Vertex vertex = vertexHashMap.get(key);
            if (vertex.getOutEdge() == 0)
            {
                deadEndsValue += pgMap.get(key) / vertexHashMap.size();
            }
        }
        return deadEndsValue;
    }
    /**
     * 通知所有的clients任务完成了，同时返回最终的结果
     */
    private void notifyClientsJobOver(HashMap<String, Double> finalResultMap)
    {
        for (int i = 0; i < clientSocketList.size(); i++)
        {
            final Socket socket = clientSocketList.get(i);
            HashMap<String, Object> resMap = new HashMap<>();
            resMap.put(Constant.CONTENT_KEY, finalResultMap);
            final Message msg = MessageBuildFactory
                    .reconstructMessage(MessageBuildFactory.SERVER_TASK_OVER, Message.SERVER_TAG, Constant.SERVER_HOST,
                            Constant.getIpAddr(socket), resMap);
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    writeData(socket, msg);
                }
            }).start();
        }
    }
    /**
     * 对client端发送的pagerank值进行合并
     *
     * @param currentPgMap
     * @param content
     */
    private synchronized void mergeResult(HashMap<String, Double> currentPgMap, HashMap<String, Double> content)
    {
        Iterator<String> contentKeyIterator = content.keySet().iterator();
        while (contentKeyIterator.hasNext())
        {
            String key = contentKeyIterator.next();
            Double value = currentPgMap.get(key);
            currentPgMap.put(key, value + content.get(key));
        }
    }
    private double computeDistance(HashMap<String, Double> currentPgMap, HashMap<String, Double> previousPgMap)
    {
        Object[] currentKeyArray = currentPgMap.keySet().toArray();
        double distance = 0.0;
        for (Object key : currentKeyArray)
        {
            distance += Math.abs(currentPgMap.get(key.toString()) - previousPgMap.get(key.toString()));
        }
        return distance;
    }
    public static void main(java.lang.String[] args)
    {
        new Server();
    }
    /**
     * 负责处理宕机的client
     *
     * @param socket
     */
    private void handleDieClient(Socket socket)
    {
        synchronized (clientSocketOperationMonitor)
        {
            String dieIpAddr = Constant.getIpAddr(socket);
            if (!lostClientsAddrSet.contains(dieIpAddr))
            {
                System.err.println("客户端" + Constant.getIpAddr(socket) + "断开连接");
                lostClientsAddrSet.add(Constant.getIpAddr(socket));
            }
            clientSocketList.remove(socket);
            clientObjInMap.remove(socket);
            clientObjOutMap.remove(socket);
        }
        try
        {
            socket.close();
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
        }
    }
    /**
     * 获取client发送的信息，如果没有发送则处于等待状态，如果抛出异常那么判断
     * client断开连接
     *
     * @param socket
     */
    @Override
    public void readData(Socket socket)
    {
        while (true)
        {
            try
            {
                ObjectInputStream objIn = clientObjInMap.get(socket);
                if (objIn != null)
                {
                    receiveData(socket, (Message) objIn.readObject());
                }
            }
            catch (IOException e)
            {
                //                e.printStackTrace();
              /*  if (e instanceof EOFException)
                {
                    continue;
                }*/
                handleDieClient(socket);
                break;
            }
            catch (ClassNotFoundException e)
            {
                e.printStackTrace();
            }
        }
    }
    @Override
    public void writeData(final Socket socket, final Message... content)
    {
        try
        {
            ObjectOutputStream objOut = clientObjOutMap.get(socket);

             /*   if(msg.length>0)
                {
                    objOut.write();

                }*/
            Message msg = null;
            if (content.length == 0)
            {
                String inputStr = new Scanner(System.in).next();
                msg = Message.restructMsg(Message.SERVER_REQUEST_OTHER_TYPE, Message.SERVER_TAG,
                        Constant.SERVER_HOST, Constant.getIpAddr(socket), inputStr);
            }
            else
            {
                msg = content[0];
            }
            objOut.writeObject(msg);
            //            System.out.println("server write:" + msg.getType() + " to " + Constant.getIpAddr(socket));
            objOut.flush();
        }
        catch (IOException e)
        {
            handleDieClient(socket);
        }
    }
    /**
     * 向所有client发送广播，同时发送一些必要的信息，比如
     * 部分图信息
     */
    private void broadcast()
    {
        if (isBroadcastThreadStart)
        {
            return;
        }
        while (true)
        {
            System.out.println("输入广播内容");
            isBroadcastThreadStart = true;
            String broadcastContent = new Scanner(System.in).next();
            if (broadcastContent.equals("start"))
            {
                int dispatchItemBeginIndex = 0;
                HashMap<String, Vertex> graphMap = PageGraphUtils.getSingleInstance().getVertexHashMap();
                Object[] keyArray = graphMap.keySet().toArray();
                for (Object key : keyArray)
                {
                    Vertex vertex = graphMap.get(key);
                    //该节点是dead end
                    previousPgMap.put(key.toString(), vertex.getPageRank());
                    updatedPgMap.put(key.toString(), 0.0);
                }
                writeSuperstepPagerankToDisk(previousPgMap, pageRankSuperstep);
                clientsCount = clientSocketList.size();
                int dispatchItemCount = -1;
                clientCyclicBarrier.set(clientsCount);
                for (int i = 0; i < clientsCount; i++)
                {
                    //                System.out.println("size:" + clientSocketList.size());
                    Socket socket = clientSocketList.get(i);
                    int extraVertexCount = graphMap.size() - dispatchItemBeginIndex;
                    int extraComputeNodeCount = clientsCount - i;
                    dispatchItemCount =
                            extraVertexCount % extraComputeNodeCount == 0 ? extraVertexCount / extraComputeNodeCount :
                                    (extraVertexCount / extraComputeNodeCount + 1);
                    if (dispatchItemBeginIndex + dispatchItemCount > graphMap.size())
                    {
                        dispatchItemCount = graphMap.size() - dispatchItemBeginIndex;
                    }
                    HashMap<String, Vertex> sendMap = dispatchNode(graphMap, dispatchItemBeginIndex, dispatchItemCount);
                    HashMap<String, Object> wrapMap = new HashMap<>();
                    wrapMap.put(Constant.CONTENT_KEY, sendMap);
                    Message msg = MessageBuildFactory
                            .reconstructMessage(broadcastContent, Message.SERVER_TAG, Constant.SERVER_HOST,
                                    Constant.getIpAddr(socket), wrapMap);
                    dispatchItemBeginIndex += dispatchItemCount;
                    //                System.out.println();
                    writeData(socket, msg);
                }
                isWorkStarted = true;
                //                notifyNormalIterateSuperstepWork(clientSocketList, updatedPgMap);
                //                clearUpdatedPgMapAfterIterate();
            }
            else if (broadcastContent.equals("over"))
            {
            }

         /*   if (!isDetectClientsAliveThreadStart)
            {
                new DetectClientsAliveThread().start();
                isDetectClientsAliveThreadStart = true;
            }*/
        }
    }
    /**
     * 提高性能，同时需要避免多线程导致当前变量内容改变
     *
     * @param pgMap
     * @param superstep
     */
    private void writeSuperstepPagerankToDisk(final HashMap<String, Double> pgMap, final int superstep)
    {
        new Thread()
        {
            @Override
            public void run()
            {
                BufferedWriter writer = null;
                Set<String> keySet = pgMap.keySet();
                File file = new File(pageRankFileParent, "pagerank" + superstep + ".txt");
                try
                {
                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
                    Iterator<String> keyIterator = keySet.iterator();
                    while (keyIterator.hasNext())
                    {
                        String key = keyIterator.next();
                        writer.write(key + "\t" + pgMap.get(key) + "\r\n");
                    }
                    writer.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }.start();
    }
    /**
     * 在开始命令client工作之前需要将当前的currentPgMap
     * key所对应的value值全部设置为0
     */
    private void clearUpdatedPgMapAfterIterate()
    {
        Iterator<String> keyIterator = updatedPgMap.keySet().iterator();
        while (keyIterator.hasNext())
        {
            updatedPgMap.put(keyIterator.next(), 0.0);
        }
    }
    /**
     * 通知所有client开始正常工作，只有当客户端存在已经分配的任务节点的时候才能使用该方法
     *
     * @param socket
     * @param updatedPgMap
     */
    private void notifyNormalIterateSuperstepWork(Socket socket, HashMap<String, Double> updatedPgMap)
    {
        if (lostClientsAddrSet.contains(Constant.getIpAddr(socket)))
        {
            return;
        }
      /*  HashMap<String, Object> wrapMap = new HashMap<>();
        wrapMap.put(Constant.PAGE_RANK_SUPERSTEP_KEY, pageRankVersion);
        wrapMap.put(Constant.CONTENT_KEY, updatedPgMap);*/
        Message msg = MessageBuildFactory
                .reconstructMessage(MessageBuildFactory.SERVER_NORMAL_ITERATE, Message.SERVER_TAG,
                        Constant.SERVER_HOST,
                        Constant.getIpAddr(socket));
        writeData(socket, msg);
    }
    /**
     * 通知所有client开始新一轮计算工作
     *
     * @param clientSocketList
     * @param updatedPgMap     更新后的pagerank值
     */
    private void notifyNormalIterateSuperstepWork(
            ArrayList<Socket> clientSocketList, HashMap<String, Double> updatedPgMap
    )
    {
        for (int i = 0; i < clientSocketList.size(); i++)
        {
            Socket socket = clientSocketList.get(i);
            notifyNormalIterateSuperstepWork(socket, updatedPgMap);
        }
    }
    private HashMap<String, Vertex> dispatchNode(
            HashMap<String, Vertex> graphMap, int dispatchItemBeginIndex, int dispatchItemCount
    )
    {
        Set<String> keySet = graphMap.keySet();
        Object[] keyArrays = keySet.toArray();
        HashMap<String, Vertex> resMap = new HashMap<>();
        for (int i = dispatchItemBeginIndex; i - dispatchItemBeginIndex < dispatchItemCount; i++)
        {
            resMap.put(keyArrays[i].toString(), graphMap.get(keyArrays[i]));
        }
        return resMap;
    }
    /**
     * 每隔10秒钟发出以此请求来检测客户端的状态,如果readData或者
     * writeData抛出IO异常，就表示对应的客户端已经死亡了
     */
    class DetectClientsAliveThread extends Thread
    {
        final static int DETECT_TIME_INTERVAL = 10000; //每间隔10s发送一个消息
        @Override
        public void run()
        {
            //            super.run();
            while (true)
            {
                for (int i = 0; i < clientSocketList.size(); i++)
                {
                    Socket socket = clientSocketList.get(i);
                    Message msg = MessageBuildFactory
                            .reconstructMessage(MessageBuildFactory.SERVER_DETECT, Message.SERVER_TAG,
                                    Constant.SERVER_HOST,
                                    Constant.getIpAddr(socket));
                    writeData(socket, msg);
                }
                try
                {
                    Thread.sleep(DETECT_TIME_INTERVAL);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }
    private Socket findSocketByHostIpAddr(String ipAddr, ArrayList<Socket> socketList)
    {
        Socket desSocket = null;
        for (int i = 0; i < socketList.size(); i++)
        {
            Socket socket = socketList.get(i);
            if (socket.getInetAddress().getHostAddress().equals(ipAddr))
            {
                desSocket = socket;
                break;
            }
        }
        return desSocket;
    }
}
