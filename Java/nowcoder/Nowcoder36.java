package nowcoder;
// 输入两个单链表，找出它们的第一个公共结点。
public class Nowcoder36{
	// 方法1:二次迭代
	// 思路:将两个链表分别遍历两次.首次遍历时,分别统计两个链表的长度,然后取长度差a-b;
	// 第二次遍历时,其中一个指针先在较长的链表上先走a-b步,然后再双指针同时前进,由于
	// 双指针到对应链表结尾的长度都相同,因此一定能够找到首个公共节点,即使没有公共节点也会
	// 因为都为null而退出循环,以此来找到首个公共节点.
	// 时间复杂度:O(n),空间复杂度:O(1),其中n为较长链表的长度
	public ListNode FindFirstCommonNode(ListNode pHead1, ListNode pHead2) {
		// 排除特殊情况
		if(pHead1 == null || pHead2 == null) return null;
		// 首次遍历
		// 统计两个链表的长度
		ListNode str1 = pHead1, str2 = pHead2;
		int len1 = 0, len2 =0;
		while(str1 != null){
			len1++;
			str1 = str1.next;
		}
		while(str2 != null){
			len2++;
			str2 = str2.next;
		}
		// 第二次遍历
		// 计算链表长度差count,先在较长的链表中走count步
		int count = len1 - len2;
		if(count >= 0){
			str1 = pHead1;
			str2 = pHead2;
		}else{
			str1 = pHead2;
			str2 = pHead1;
			count = -count;
		}
		while(count-->0){
			str1 = str1.next;
		}
		// 两个指针同时前进,直到指针指向相同地址
		while(str1 != str2){
			str1 = str1.next;
			str2 = str2.next;
		}
		// 返回首个公共节点
		return str1;
	}
}