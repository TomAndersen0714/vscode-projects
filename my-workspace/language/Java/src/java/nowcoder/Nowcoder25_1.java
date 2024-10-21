package java.nowcoder;
// 输入一个复杂链表（每个节点中有节点值，以及两个指针，一个指向下一个节点，
// 另一个特殊指针指向任意一个节点），返回结果为复制后复杂链表的head。
// （注意，输出结果中请不要返回参数中的节点引用，否则判题程序会直接返回空）
public class Nowcoder25_1{
    // 方法2:通过next指针遍历原数组,遍历的同时将当前节点的clone节点链接到
    // 当前节点与当前节点的next节点之间,直到遍历结束,此时原始节点和clone节
    // 点形成了新的链表;然后通过next指针进行第二次遍历,每次遍历时将当前节点
    // 的下一个节点(即当前节点的clone节点)的random指针指向当前节点的random
    // 节点(如果当前节点的random指针为null,则下一个节点的random也为null);
    // 保存原始头节点的下一个节点,即克隆链表的头节点.
    // 通过next指针第三次遍历链表,每次遍历时将当前节点的clone节点(即next节点)
    // 保存,同时将当前节点的next指针指向next.next,然后将当前节点指向next
    // 如果当前节点的clone节点的next不为null,则也将clone节点的next指针指向
    // next.next.最后的最后返回克隆链表的头节点.
    public RandomListNode Clone(RandomListNode pHead){
        // 排除特殊情况
        if(pHead == null) return null;
        // 第一次通过next指针遍历链表,遍历的同时链接clone节点
        RandomListNode str1 = pHead, str2;
        while(str1 != null){
            str2 = new RandomListNode(str1.label);
            str2.next = str1.next;
            str1.next = str2;
            str1 = str2.next;
        }
        // 第二次通过next指针遍历链表,遍历的同时链接random节点
        str1 = pHead;
        while(str1 != null){
            if(str1.random == null) str1.next.random = null;
            else str1.next.random = str1.random.next;
            str1 = str1.next.next;
        }
        // 第三次通过next指针遍历链表,遍历的同时分离原链表和clone链表
        RandomListNode cloneHead = pHead.next;
        str1 = pHead;
        while(str1 != null){
            str2 = str1.next;
            str1.next = str2.next;
            if(str2.next != null) str2.next = str2.next.next;
            str1 = str1.next;
        }
        // 返回克隆链表头
        return cloneHead;
    }
}