package nowcoder;
// 输入两个单链表，找出它们的第一个公共结点。
public class Nowcoder36_1{
	// 方法2:双指针循环遍历
	// 思路:使用两个指针分别指向两个链表头,若两个指针的值不相同则进行遍历.
	// 当其中一个指针(如str1)先到达结尾时,则将其指向另一个(较长)链表的头,然后
	// 继续遍历,当另一个指针(如str2)也到达结尾时,则也将其指向另一个(较短)链表的头.
	// 此时前一个指针(如str1)在当前长链表中已经走过的长度,为两个链表的长度差,因此
	// 两个指针的剩余长度相同.若双指针继续进行遍历,那么一定能够找到公共节点(若没有
	// 则都为null),即退出循环,最后返回其中一个指针的地址即可.
	// 时间复杂度:O(n),空间复杂度:O(1),其中n为较长链表的长度
	public ListNode FindFirstCommonNode(ListNode pHead1, ListNode pHead2) {
		// 排除特殊情况
		if(pHead1 == null || pHead2 == null) return null;
		// 使用双指针进行循环遍历
		ListNode str1 = pHead1, str2 = pHead2;
		while(str1 != str2){
			str1 = str1==null?pHead2:str1.next;
			str2 = str2==null?pHead1:str2.next;
		}
		// 返回指针值
		return str1;
	}
}