package nowcoder;
import java.util.HashMap;
// 输入一个复杂链表（每个节点中有节点值，以及两个指针，一个指向下一个节点，
// 另一个特殊指针指向任意一个节点），返回结果为复制后复杂链表的head。
// （注意，输出结果中请不要返回参数中的节点引用，否则判题程序会直接返回空）

// 题目分析:原链表中next指针将原始节点链接成一个单链表,但是random节点能够指向任何
// 节点,包括自身,因此无法通过random进行遍历原始链表
public class Nowcoder25{
    // 方法1:使用HashMap存储原始链表节点及其对应的clone节点.第一次通过next指针
    // 遍历原始链表,创建每个节点的clone节点的同时将clone节点链接成单链表.第二次
    // 通过next节点遍历原始链表,若当前节点的random节点为null,则将当前clone节点
    // 的random节点也置成null,否则就去HashMap中寻找当前节点的random节点对应的clone节点
    // 并将当前节点的clone节点的random指针指向其random节点对应的clone节点.
    public RandomListNode Clone(RandomListNode pHead){
        // 排除特殊情况
        if(pHead == null) return null;
        // 创建HashMap用于存储原始节点及其对应clone节点
        HashMap<RandomListNode,RandomListNode> map = new HashMap<>();
        // 创建clone头节点,并将原始头与clone头加入到HashMap中
        RandomListNode cloneHead = new RandomListNode(pHead.label);
        map.put(pHead,cloneHead);
        // 通过next指针遍历原始链表,同时将next节点与其对应的clone节点加入到map中
        RandomListNode str1 = pHead,str2 = cloneHead;
        while(str1.next != null){
            str2.next = new RandomListNode(str1.next.label);
            map.put(str1.next,str2.next);
            str1 = str1.next;
            str2 = str2.next;
        }
        // 通过next指针再次遍历原始链表,链接random指针
        str1 = pHead;
        str2 = cloneHead;
        while(str1!=null){
            if(str1.random == null){
                str2.random = null;
            }else{
                str2.random = map.get(str1.random);
            }
            str1 = str1.next;
            str2 = str2.next;
        }
        // 返回clone头节点
        return cloneHead;
    }
}


