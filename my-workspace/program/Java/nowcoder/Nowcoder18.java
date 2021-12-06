package nowcoder;
public class Nowcoder18{
	// 操作给定的二叉树，将其变换为源二叉树的镜像。
	// 方法1：递归
	public void Mirror(TreeNode root) {
		// 排除特殊情况
		if(root == null) return;
		// 交换当前节点的左右子树
		TreeNode temp = root.left;
		root.left = root.right;
		root.right = temp;
		// 递归交换子树的子树
		Mirror(root.left);
		Mirror(root.right);
	}
}