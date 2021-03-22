package nowcoder;

public class Nowcoder14{
	// 输入一个链表，输出该链表中倒数第k个结点。
	public ListNode FindKthToTail(ListNode head,int k) {
		// 排除特殊情况
		if(head == null) return null;
		// 一般情况,使用双指针(快慢指针)
		ListNode slow = head, fast = head;
		int i=k-1;
		while(i>0 && fast.next!=null){
			fast = fast.next;
			i--;
		}
		if(i!=0) return null;
		while(fast.next!=null){
			fast = fast.next;
			slow = slow.next;
		}
		return slow;
	}
}