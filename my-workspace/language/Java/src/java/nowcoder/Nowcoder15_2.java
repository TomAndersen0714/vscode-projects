package java.nowcoder;
public class Nowcoder15_2{
	// 单链表的反转
	// 方法三:使用递归
	// 每次递归时保存当前节点和顺序下一个节点,然后对下一个及之后的节点进行反转
	// 每次迭代结束返回子链表反转后的新的头结点,然后将当前节点链接到之前的下一
	// 个节点之后,并返回新的头结点
	public ListNode ReverseList(ListNode head) {
		// 排除特殊情况,同时作为递归的出口之一
		if(head == null || head.next == null) return head;
		// 一般情况
		ListNode cur = head, next = head.next;
		ListNode newHead = ReverseList(next);
		cur.next = null;
		next.next = cur;
		return newHead;
	}
}