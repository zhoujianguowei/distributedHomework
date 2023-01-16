package utils;

import sun.nio.cs.ext.MacHebrew;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static javafx.scene.input.KeyCode.M;
/**
 * Created by Administrator on 2016/6/28.
 * 一些常量定义
 */
public class Constant
{
    public static final String SERVER_HOST = "192.168.137.1"; //服务器地址
    public static final int SERVER_PORT = 9999; //服务器端口
    /**
     * 迭代终止条件
     */
    public static final double CONVERGENCE_DIFF = Math.pow(10, -6);
    /**
     * pagerank计算的迭代次数
     */
    public static final String PAGE_RANK_SUPERSTEP_KEY = "pagerank_version_key"; //pageRank superstep次数
    /**
     * 网络输出内容所对应的key，传输的实际内容是map类型
     */
    public static final String CONTENT_KEY = "content_key"; //网络中传输的内容key
    /**
     * 获取socket所绑定的client的地址
     *
     * @param socket server端负责处理client通信的socket
     * @return
     */
    public static String getIpAddr(Socket socket)
    {
        InetAddress inetAddress = socket.getInetAddress();
        return inetAddress.getHostAddress();
    }
    public static void main(String[] args)
    {
        String moduleName = "distributedHomework";
        try
        {
            for (int i = 0; i < 30; i++)
            {
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(new FileInputStream(
                                new File(moduleName + File.separator + "src" + File.separator + "pagerank" + i + "" +
                                        ".txt"))));
                String readLine = null;
                double sum = 0;
                while ((readLine = reader.readLine()) != null)
                {
                    readLine = readLine.trim();
                    if (readLine.startsWith("#") || readLine.length() == 0)
                    {
                        continue;
                    }
                    Matcher matcher = Pattern.compile("(\\S+)\\s(\\S+)").matcher(readLine);
                    if (matcher.find())
                    {
                        sum += Double.parseDouble(matcher.group(2));
                    }
                }
                System.out.println("superstep" + i + ":=" + sum);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
