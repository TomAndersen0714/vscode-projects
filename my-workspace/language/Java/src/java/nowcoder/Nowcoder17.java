package java.nowcoder;
public class Nowcoder17{
	// 输入两棵二叉树A，B，判断B是不是A的子结构。（ps：我们约定空树不是任意一个树的子结构）
    // 方法1:递归
	public boolean HasSubtree(TreeNode root1,TreeNode root2) {
		// 排除意外情况
		if(root2 == null || root1 ==null) return false;
        // 如果两个节点的值相同则继续递归,并保存结果
        boolean result = false;
        // 如果root1和root2的值相同,则判断root2的左右子树是否为root1的左右子树的同根子树
        if(root1.val == root2.val){
            result = isSameRootSubtree(root1.left,root2.left) 
                && isSameRootSubtree(root1.right,root2.right);
        }
        // 如果之前的方式判断出不是子树,则再尝试向root1的左右子树中递归判断
        if(!result) result = HasSubtree(root1.left,root2);
        if(!result) result = HasSubtree(root1.right,root2);
        return result;
	}
	
    // 递归遍历树2是否是树1的同根子树
	public boolean isSameRootSubtree(TreeNode root1,TreeNode root2){
		// 排除意外情况
        if(root1 == root2 || root2 == null) return true;
        if(root1 == null) return false;
        // 如果两个节点的值相同则继续递归,否则返回false
        if(root1.val == root2.val){
            return isSameRootSubtree(root1.left,root2.left) 
                && isSameRootSubtree(root1.right,root2.right);
        }
        return false;
	}
}
