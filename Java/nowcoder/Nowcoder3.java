package nowcoder;
import java.util.ArrayList;
import java.util.Stack;


// 输入一个链表，按链表从尾到头的顺序返回一个ArrayList
public class Nowcoder3{
    // 使用Stack保存链表值,最后将其依次推出放入ArrayList中
    public ArrayList<Integer> printListFromTailToHead(ListNode listNode) {
        ArrayList<Integer> list = new ArrayList<>();
        Stack<Integer> stack = new Stack<>();
        while(listNode!=null){
            stack.push(listNode.val);
            listNode = listNode.next;
        }
        while(!stack.isEmpty()){
            list.add(stack.pop());
        }
        return list;
    }
}