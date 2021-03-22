package nowcoder;
public class Nowcoder16_1{
	// 输入两个单调递增的链表，输出两个链表合成后的链表，当然我们需要合成后的链表满足单调不减规则。
	// 方法2：非递归,不改变原有链表,使用新建链表
	public ListNode Merge(ListNode list1,ListNode list2) {
		// 定义新链表头
		ListNode mergeHead = new ListNode(-1);
		ListNode cur = mergeHead;
		// 迭代原始列表
		while(list1!=null && list2!=null){
			while(list1!=null && list1.val <= list2.val){
				cur.next = new ListNode(list1.val);
				cur = cur.next;
				list1 = list1.next;
			}
			// 如果list1遍历完成则直接退出
			if(list1 == null) break;
			while(list2!=null && list2.val <= list1.val){
				cur.next  = new ListNode(list2.val);
				cur = cur.next;
				list2 = list2.next;
			}
		}
		// 若list1还未遍历完成,则直接全部加入到新链表尾部
		while(list1!=null){
			cur.next = new ListNode(list1.val);
			cur = cur.next;
			list1 = list1.next;
		}
		// 若list2还未遍历完成,则直接全部加入到新链表尾部
		while(list2!=null){
			cur.next = new ListNode(list2.val);
			cur = cur.next;
			list2 = list2.next;
		}
		return mergeHead.next;
	}
}
