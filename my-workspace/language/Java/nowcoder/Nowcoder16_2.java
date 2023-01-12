package nowcoder;
public class Nowcoder16_2{
	// 输入两个单调递增的链表，输出两个链表合成后的链表，当然我们需要合成后的链表满足单调不减规则。
	// 方法3：递归
	// 每次比较两个链表头的对应值,将小的加入保存,然后继续向下递归,保证每次递归后返回一个有序链表
	public ListNode Merge(ListNode list1,ListNode list2) {
		// 排除特殊情况,同时设置一种递归出口
		if(list1 == null) return list2;
		if(list2 == null) return list1;
		// 进行递归
		ListNode temp;
		if(list1.val <= list2.val){
			temp = Merge(list1.next,list2);
			list1.next = temp;
			return list1;
		}else{
			temp = Merge(list1,list2.next);
			list2.next = temp;
			return list2;
		}
	}
}