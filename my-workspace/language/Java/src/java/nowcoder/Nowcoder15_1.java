package java.nowcoder;
public class Nowcoder15_1{
	// 单链表的反转
	// 方法二:使用三指针迭代
	public ListNode ReverseList(ListNode head) {
		// 排除特殊情况
		if(head == null || head.next == null) return head;
		// 一般情况
		ListNode left=head, mid=head, right=head.next;
		while(right!=null){
			mid.next = right.next;
			right.next = left;
			left = right;
			right = mid.next;
		}
		return left;
	}
}
