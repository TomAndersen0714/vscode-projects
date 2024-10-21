package java.nowcoder;
public class Nowcoder15{
	// 单链表的反转
	// 方法一:使用头插法迭代
	public ListNode ReverseList(ListNode head) {
		// 排除特殊情况
		if(head == null || head.next == null) return head;
		// 一般情况
		ListNode newHead = new ListNode(-1);
		ListNode temp;
		while(head!=null){
			temp = head.next;
			head.next = newHead.next;
			newHead.next = head;
			head = temp;
		}
		return newHead.next;
	}
	
}
