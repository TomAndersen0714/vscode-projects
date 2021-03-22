package nowcoder;
import java.util.Queue;
import java.util.LinkedList;
// 求二叉树深度
public class Nowcoder38_1{
	// 方法2:队列+BFS
	// 思路:使用队列进行层次遍历.若root节点不为null,则将其加入到队列中,
	// 然后当队列非空时就进行层次遍历,每次遍历时,层次+1.直到遍历结束,返回层次.
	// 时间复杂度:O(n),其中n为节点个数
	public int TreeDepth(TreeNode root) {
		// 排除特殊情况
		if(root == null) return 0;
		// 创建辅助队列
		Queue<TreeNode> queue = new LinkedList<>();
		queue.offer(root);
		int size,level = 0;
		TreeNode tmp;
		// 层次遍历
		while(!queue.isEmpty()){
			level++;
			size = queue.size();
			while(size-->0){
				tmp = queue.poll();
				if(tmp.left != null) queue.offer(tmp.left);
				if(tmp.right != null) queue.offer(tmp.right);
			}
		}
		// 返回层次
		return level;
	}
}