package java.nowcoder;
// 判断是否是平衡二叉树
public class Nowcoder39{
	// 题目分析:平衡二叉树的限定条件为左右子树的高度差不能超过1
	// 方法1:递归
	// 思路:若当前节点为null,则直接返回true,否则递归判断左右子树是否满足平衡二叉树.
	// 先判断左子树是否满足平衡二叉树,向下遍历的同时获取子树的高度并保存,然后递归
	// 右子树判断其是否满足平衡二叉树,并获取其高度,最后计算两个高度差,若差值的绝对值
	// 大于1,则放回false,否则返回true;
	// 时间复杂度:O(n)
	private int height = 0;
	public boolean IsBalanced_Solution(TreeNode root) {
		// 排除特殊情况
		if(root == null) {
			height = 0;
			return true;
		};
		// 否则递归左右子树
		// 先递归左子树,如果左子树不为平衡二叉树,则直接返回false
		if(!IsBalanced_Solution(root.left)) return false;
		// 左子树为平衡二叉树,则保存其高度,然后递归右子树
		int left = height;
		if(!IsBalanced_Solution(root.right)) return false;
		int right = height;
		// 计算高度差,若大于1则返回false,否则计算当前节点高度并返回true
		int diff = left-right;
		if(diff<-1 || diff >1) return false;
		height = Math.max(left,right)+1;
		return true;
	}
}