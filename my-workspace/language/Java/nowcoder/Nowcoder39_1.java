package nowcoder;
// 判断是否是平衡二叉树
public class Nowcoder39_1{
	// 题目分析:平衡二叉树的限定条件为左右子树的高度差不能超过1
	// 方法2:递归
	// 思路:和方法1不同,不再使用成员变量保存子树高度,而是直接声明新的方法
    // 声明辅助函数getHeight用于计算树的高度,若返回-1则表示当前树不平衡.
    // 辅助函数getHeight采用递归计算树的高度,首先递归左子树,若其高度为-1
    // 则表明左子树不平衡,直接返回-1,否则保存其高度,然后递归右子树高度,若
    // 右子树高度为-1,则表明右子树不平衡,直接返回-1,否则保存其高度,然后计算
    // 两个子树的高度差,若高度差的绝对值大于1,则表明当前节点不平衡,直接返回-1
    // 否则返回左右子树高度较大值+1
	public boolean IsBalanced_Solution(TreeNode root) {
		// 排除特殊情况
		if(root == null) return true;
		// 进行递归计算当前树的高度,若返回-1则表示不平衡
		return getHeight(root)==-1?false:true;
	}
	
	// 递归计算子树高度,同时判断是否平衡,若不平衡则返回-1,否则返回真实高度
	private int getHeight(TreeNode root){
		// 排除特殊情况
		if(root == null) return 0;
		// 计算左子树高度,若为-1则直接返回-1
		int left = getHeight(root.left);
		if(left == -1) return -1;
		// 计算右子树高度,若为-1则直接返回-1
		int right = getHeight(root.right);
		if(right == -1) return -1;
		// 计算左右子树高度差,若差值大于1则返回-1
		int diff = left-right;
		if(diff<-1 || diff>1) return -1;
		// 否则返回真实高度
		return Math.max(left,right)+1;
	}
}