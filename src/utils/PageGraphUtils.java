package utils;

import entity.Vertex;

import java.io.*;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Created by Administrator on 2016/6/28.
 * 从文件中读取原始数据构造相应的图，图的表示形式采用邻接表
 */
public class PageGraphUtils
{
    private static PageGraphUtils singleInstance;
    private File originFile;
    /**
     * 每个节点用一个字符串来标识
     */
    private HashMap<String, Vertex> vertexHashMap;
    private PageGraphUtils(File originFile)
    {
        this.originFile = originFile;
        vertexHashMap = new HashMap<>();
    }
    public File getOriginFile()
    {
        return originFile;
    }
    public HashMap<String, Vertex> getVertexHashMap()
    {
        restructureGraph();
        return vertexHashMap;
    }
    public void setOriginFile(File originFile)
    {
        this.originFile = originFile;
        vertexHashMap.clear();
    }
    public static synchronized PageGraphUtils getSingleInstance()
    {
        if (singleInstance == null)
        {
            singleInstance =
                    new PageGraphUtils(new File("distributedHomework" + File.separator + "src" + File.separator + "Wiki-Vote.txt"));
        }
        return singleInstance;
    }
    /**
     * 从本地文件获取图信息
     */
    private void restructureGraph()
    {
        if (vertexHashMap.size() > 0)
        {
            return;
        }
        BufferedReader reader = null;
        String readLine = null;
        try
        {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(originFile)));
            while ((readLine = reader.readLine()) != null)
            {
                readLine = readLine.trim();
                if (readLine.length() == 0 || readLine.startsWith("#"))
                {
                    continue;
                }
                Pattern pattern = Pattern.compile("(\\d+)\\s+(\\d+)");
                Matcher matcher = pattern.matcher(readLine);
                if (matcher.matches())
                {
                    Vertex vertex = new Vertex();
                    String srcVertexName = matcher.group(1);//有向边弧尾
                    String desVertexName = matcher.group(2);//有向边的弧头
                    vertex.setName(srcVertexName);
                    //计算出度
                    Vertex srcVertex = vertexHashMap.get(srcVertexName);
                    if (srcVertex != null)
                    {
                        srcVertex.setOutEdge(srcVertex.getOutEdge() + 1); //出度加1
                        srcVertex.getOutEdgesNameList().add(desVertexName);
                    }
                    else
                    {
                        vertex.setOutEdge(1);
                        vertex.getOutEdgesNameList().add(desVertexName);
                        vertexHashMap.put(srcVertexName, vertex);
                    }
                    //计算入度
                    Vertex desVertex = vertexHashMap.get(desVertexName);
                    if (desVertex != null)
                    {
                        desVertex.setInEdge(desVertex.getInEdge() + 1);
                    }
                    else
                    {
                        desVertex = new Vertex();
                        desVertex.setName(desVertexName);
                        desVertex.setInEdge(1);
                        vertexHashMap.put(desVertexName, desVertex);
                    }
                }
            }
            reader.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    public static void main(String[] args)
    {
        singleInstance = getSingleInstance();
        singleInstance.getVertexHashMap();
    }
}
