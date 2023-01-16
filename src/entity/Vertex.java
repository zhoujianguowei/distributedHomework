package entity;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Administrator on 2016/6/28.
 * 图的节点信息，包括出度节点列表，入度以及该节点的pagerank值
 */
public class Vertex implements Serializable
{
    private static final long serialVersionUID=785721098735213401L;
    String name;   //顶点名称
    int outEdge; //出边个数
    int inEdge; //入边个数
    ArrayList<String> outEdgesNameList; //通过出边名称查找对应的顶点

    double pageRank;//pageRank的值

    public Vertex(double pageRank)
    {
        this.pageRank = pageRank;
        outEdgesNameList = new ArrayList<>();
    }

    @Override
    public String toString()
    {
        return "Vertex{" +
                "name='" + name + '\'' +
                ", outEdge=" + outEdge +
                ", inEdge=" + inEdge +
                ", outEdgesNameList=" + outEdgesNameList +
                ", pageRank=" + pageRank +
                '}';
    }

    public Vertex()
    {
        this(1.0);
    }

    public ArrayList<String> getOutEdgesNameList()
    {
        return outEdgesNameList;
    }

    public void setOutEdgesNameList(ArrayList<String> outEdgesNameList)
    {
        this.outEdgesNameList = outEdgesNameList;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getOutEdge()
    {
        return outEdge;
    }

    public void setOutEdge(int outEdge)
    {
        this.outEdge = outEdge;
    }

    public int getInEdge()
    {
        return inEdge;
    }

    public void setInEdge(int inEdge)
    {
        this.inEdge = inEdge;
    }

    public double getPageRank()
    {
        return pageRank;
    }

    public void setPageRank(double pageRank)
    {
        this.pageRank = pageRank;
    }
}
