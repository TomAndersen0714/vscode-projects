package nowcoder;
// 求二叉树深度
public class Nowcoder38{
	// 方法1:方法栈+DFS
	// 思路:若当前节点为null则直接返回0,否则递归左右子树,计算其深度,取较大者+1进行返回
	// 时间复杂度:O(n),其中n为节点个数
	public int TreeDepth(TreeNode root) {
		// 设置递归出口1
		if(root == null) return 0;
		// 若当前节点不为null,则递归计算左右子树深度,取较大者+1返回
		return Math.max(TreeDepth(root.left),TreeDepth(root.right))+1;
	}
}