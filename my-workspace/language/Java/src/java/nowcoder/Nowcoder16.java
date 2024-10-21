package java.nowcoder;
public class Nowcoder16{
	// 输入两个单调递增的链表，输出两个链表合成后的链表，当然我们需要合成后的链表满足单调不减规则。
	// 方法1：非递归,直接将原始链表拆分组装
	public ListNode Merge(ListNode list1,ListNode list2) {
        // 定义新链表头
        ListNode mergedHead = new ListNode(-1);
        ListNode cur = mergedHead,temp;
        // 遍历list1和list2
        while(list1!=null && list2!=null){
            while(list1!=null && list1.val <= list2.val){
                temp = list1.next;
                list1.next = null;
                cur.next = list1;
                cur = cur.next;
                list1 = temp;
            }
            if(list1==null) break;
            while(list2!=null && list2.val <= list1.val){
                temp = list2.next;
                list2.next = null;
                cur.next = list2;
                cur = cur.next;
                list2 = temp;
            }
        }
        while(list1!=null){
            temp = list1.next;
            list1.next = null;
            cur.next = list1;
            cur = cur.next;
            list1 = temp;
        }
        while(list2!=null){
            temp = list2.next;
            list2.next = null;
            cur.next = list2;
            cur = cur.next;
            list2 = temp;
        }
        return mergedHead.next;
    }
}

