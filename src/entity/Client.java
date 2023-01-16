package entity;

import utils.Constant;
import utils.PageGraphUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/6/29.
 */
public class Client extends Node
{
    ReentrantLock lock;
    Condition computeCondition;
    int localPgVersion = 0;
    int serverPgVersion = 0;
    private String portionGraphFileName = "portionGraph.txt"; //server端分配的计算节点
    HashMap<String, Vertex> portionVertexGraph; //服务端分配的部分图结构
    HashMap<String, Double> serverPgMap; //服务端获得最新的pageRank的value值
    HashMap<String, Double> localPortionPgMap;//本地所能计算的节点以及对应pageRank值
    private Socket clientSocket;
    private ObjectInputStream objIn;
    private ObjectOutputStream objOut;
    private volatile boolean isJobFinished;

    public Client()
    {
        try
        {
            clientSocket = new Socket(Constant.SERVER_HOST, Constant.SERVER_PORT);
            objIn = new ObjectInputStream(clientSocket.getInputStream());
            objOut = new ObjectOutputStream(clientSocket.getOutputStream());
            lock = new ReentrantLock();
            computeCondition = lock.newCondition();
            localPortionPgMap = new HashMap<>();
            new Thread()
            {
                @Override
                public void run()
                {
                    readData(clientSocket);
                }
            }.start();

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 不要阻塞当前读线程，所有耗时操作统统另起线程
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

                if (isJobFinished)
                {
                    break;
                }
                Message recMsg = (Message) objIn.readObject();
                if (recMsg == null)
                {
                    continue;
                }
//                System.out.println("client receive data:" + recMsg.getType());
                Serializable recContent = recMsg.getContent();
                switch (recMsg.getType())
                {
                    case Message.SERVER_REQUEST_START_WORK_TYPE:
                        portionVertexGraph =
                                (HashMap<String, Vertex>) ((HashMap<String, Object>) recContent).get
                                        (Constant.CONTENT_KEY);

                        File portionGraphFile = new File(
                                "." + File.separator + "src" + File.separator +
                                        portionGraphFileName);
                        if (!portionGraphFile.exists())
                        {
                            BufferedWriter writer = new BufferedWriter(new
                                    OutputStreamWriter(
                                    new FileOutputStream(portionGraphFile)));
                            Set<String> vertexKeySet = portionVertexGraph.keySet();
                            Iterator<String> keyIterator = vertexKeySet.iterator();
                            while (keyIterator.hasNext())
                            {
                                String vertexKey = keyIterator.next();
                                ArrayList<String> outEdgeList =
                                        portionVertexGraph.get
                                                (vertexKey).getOutEdgesNameList();
                                for (String outEdgeKey : outEdgeList)
                                {
                                    writer.write(vertexKey + "\t" + outEdgeKey + "\r\n");
                                    if (localPortionPgMap.containsKey(outEdgeKey))
                                    {
                                        continue;
                                    }
                                    else
                                    {
                                        localPortionPgMap.put(outEdgeKey, 0.0);
                                    }
                                }
                            }
                            writer.close();
                        }
                        //request the newest pagerank value
                        Message sendMsg = MessageBuildFactory
                                .reconstructMessage
                                        (MessageBuildFactory.CLIENT_REQUEST, Message.CLIENT_TAG,
                                                InetAddress.getLocalHost().getHostAddress(),
                                                Constant.SERVER_HOST);
                        writeData(socket, sendMsg);
                        break;
                    case Message.SERVER_REQUEST_NORMAL_ITERATE:
                        sendMsg = MessageBuildFactory
                                .reconstructMessage
                                        (MessageBuildFactory.CLIENT_REQUEST, Message.CLIENT_TAG,
                                                InetAddress.getLocalHost().getHostAddress(),
                                                Constant.SERVER_HOST);
                        writeData(socket, sendMsg);
                        break;
                    case Message.SERVER_RESPONSE_PG_TYPE:
                        serverPgMap = (HashMap<String, Double>) ((HashMap<String, Object>) recContent).get
                                (Constant.CONTENT_KEY);
                        serverPgVersion = (int) ((HashMap<String, Object>) recContent).get
                                (Constant.PAGE_RANK_SUPERSTEP_KEY);
                        new Thread(
                        )
                        {
                            @Override
                            public void run()
                            {
                                compute();
                            }
                        }.start();

                        break;
                    //任务完成指令，断开连接
                    case Message.SERVER_REQUEST_TASK_OVER_TYPE:
                        HashMap<String, Double> finalPgResultMap =
                                (HashMap<String, Double>) ((HashMap<String, Object>) recContent)
                                        .get(Constant.CONTENT_KEY);
                        writeComputedPortionGraphFile(finalPgResultMap, -1, new File("finalResult.txt"));
                        System.err.println(InetAddress.getLocalHost().getHostAddress() + "本地计算完成");
                        socket.shutdownInput();
                        socket.shutdownOutput();
                        socket.close();
                        isJobFinished = true;
                        break;
                }

            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            catch (ClassNotFoundException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * 进行本地节点的计算
     */
    private void compute()
    {
        BufferedReader localReader = null;
        //客户端是否有上次计算的数据
        File localFile =
                new File("." + File.separator + "src" + File.separator + "superstep" +
                        (serverPgVersion + 1) + ".txt");
        if (portionVertexGraph == null || portionVertexGraph.size() == 0)
        {
            PageGraphUtils utils = PageGraphUtils.getSingleInstance();
            utils.setOriginFile(new File(
                    "." + File.separator + "src" + File.separator +
                            portionGraphFileName));
            portionVertexGraph = utils.getVertexHashMap();
        }
        if (localFile.exists())
        {
            System.out.println("恢复superstep" + (serverPgVersion + 1) + "数据到内存");
            localPortionPgMap.putAll(parseLocalPortionPgFile(localFile));
        }
        else
        {
            if (localPortionPgMap.size() == 0)
            {
                try
                {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new
                            FileInputStream(
                            new File(("." + File.separator + "src" + File.separator +
                                    portionGraphFileName)))));
                    String readLine = null;
                    while ((readLine = reader.readLine()) != null)
                    {
                        readLine = readLine.trim();
                        if (readLine.length() == 0 || readLine.startsWith("#"))
                        {
                            return;
                        }
                        if (readLine.startsWith("#"))
                        {
                            continue;
                        }
                        Pattern pattern = Pattern.compile("(\\S+)\\s+(\\S+)");
                        Matcher matcher = pattern.matcher(readLine);
                        if (matcher.find())
                        {
                            localPortionPgMap.put(matcher.group(2), 0.0);
                        }
//                        localPortionPgMap.put(readLine, 0.0);
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            Set<String> keySet = localPortionPgMap.keySet();
            Iterator<String> keyIterator = keySet.iterator();
            while (keyIterator.hasNext())
            {
                localPortionPgMap.put(keyIterator.next(), 0.0);
            }
            Set<String> portionVertexGraphKey = portionVertexGraph.keySet();
            Iterator<String> portionVertexGraphIterator = portionVertexGraphKey.iterator();
            while (portionVertexGraphIterator.hasNext())
            {
                String key = portionVertexGraphIterator.next();
                Vertex vertex = portionVertexGraph.get(key);
                Double serverVertexPg = serverPgMap.get(key);
                ArrayList<String> outEdgesNameList = vertex.getOutEdgesNameList();
                for (String outEdgeKey : outEdgesNameList)
                {
                    Double pg = localPortionPgMap.get(outEdgeKey);
                    localPortionPgMap.put(outEdgeKey, pg + serverVertexPg /
                            vertex.getOutEdge());
                }
            }

        }
        localPgVersion = serverPgVersion + 1;

        writeComputedPortionGraphFile(localPortionPgMap, localPgVersion);
        try
        {

            /**
             * 特别主要，为了提高序列化速度，对同一对象进行序列化时候
             * 如果对象没有改变（地址没变，对象的值可以改变），还是发送之前的
             * 序列化对象
             */
            HashMap<String, Double> tempPgMap = new HashMap<>(localPortionPgMap);
            writeData(clientSocket, MessageBuildFactory
                    .reconstructMessage(MessageBuildFactory.CLIENT_COMMIT,
                            Message.CLIENT_TAG,
                            InetAddress.getLocalHost().getHostAddress(),
                            Constant.SERVER_HOST, tempPgMap));
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
        }

    }

    /**
     * 将计算数据写入到本地磁盘
     *
     * @param localPortionPgMap
     */
    public static void writeComputedPortionGraphFile(
            Map<String, Double> localPortionPgMap,
            int pgVersion, File... renewFile
    )
    {
        Set<String> keySet = localPortionPgMap.keySet();
        Iterator<String> keyIterator = keySet.iterator();
        BufferedWriter writer = null;
        File portionGraphFile = new File(
                "." + File.separator + "src" + File.separator +
                        "superstep" + pgVersion + ".txt");
        if (renewFile.length > 0)
        {
            portionGraphFile = renewFile[0];
        }
//        if(portionGraphFile.exists())
        try
        {
            portionGraphFile.createNewFile();
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream
                    (portionGraphFile)));
            while (keyIterator.hasNext())
            {
                String key = keyIterator.next();
                writer.write(key + "\t" + localPortionPgMap.get(key) + "\r\n");
            }
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static HashMap<String, Double> parseLocalPortionPgFile(File file)
    {
        HashMap<String, Double> recMap = new HashMap<>();
        BufferedReader reader = null;
        String readLine = null;
        try
        {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            while ((readLine = reader.readLine()) != null)
            {
                readLine = readLine.trim();
                if (readLine.startsWith("#"))
                {
                    continue;
                }
                Pattern pattern = Pattern.compile("(\\S+)\\s+(\\S+)");
                Matcher matcher = pattern.matcher(readLine);
                if (matcher.find())
                {
                    String vertexKey = matcher.group(1);
                    Double portionPgValue = Double.valueOf(matcher.group(2));
                    recMap.put(vertexKey, portionPgValue);
                }


            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return recMap;
    }

    @Override
    public void writeData(final Socket socket, final Message... content)
    {

        try
        {
             /*   if(msg.length>0)
                {
                    objOut.write();

                }*/
            if (objOut == null)
            {
                synchronized (Client.this)
                {
                    objOut = new ObjectOutputStream(socket.getOutputStream());
                }
            }
            Message msg = content[0];
                  /*  if (content.length == 0)
                    {
                        String inputStr = new Scanner(System.in).next();
                        msg = Message.restructMsg(Message.SERVER_REQUEST_OTHER_TYPE,
Message.CLIENT_TAG,
                                InetAddress.getLocalHost().getHostAddress(),
Constant.getIpAddr(socket), inputStr);

                    }
                    else
                    {
                        msg = content[0];
                    }*/
            objOut.writeObject(msg);
//            System.out.println("client write data:" + msg.getType());
            objOut.flush();


        }
        catch (IOException e)
        {
            e.printStackTrace();
        }


    }


    public static void main(String[] args)
    {
        new Client();
    }


}
