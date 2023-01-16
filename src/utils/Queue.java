package utils;
import java.util.LinkedList;
class Queue<T>
{

    // T element;
    LinkedList<T> linkedList;
    public boolean isEmpty()
    {
        return linkedList.isEmpty();
    }
    public Queue()
    {
        linkedList = new LinkedList<T>();
    }
    public void enQueue(T t)
    {
        linkedList.addLast(t);
    }
    public T deQueue()
    {
        if (linkedList.isEmpty())
            return null;
        else
        {
            return linkedList.pollFirst();
        }
    }
}