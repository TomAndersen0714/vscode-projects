package nowcoder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
// 从上往下打印出二叉树的每个节点，同层节点从左至右打印。
public class Nowcoder22{
    // 方法1:层次遍历二叉树,使用队列
    public ArrayList<Integer> PrintFromTopToBottom(TreeNode root) {
        // 创建返回列表
        ArrayList<Integer> res = new ArrayList<>();
        // 排除特殊情况
        if(root == null) return res;
        // 创建层次遍历队列,并添加root节点
        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);
        // 层次遍历
        int size;
        TreeNode temp;
        while(!queue.isEmpty()){
            size = queue.size();
            while(size-- > 0){
                temp = queue.poll();
                res.add(temp.val);
                if(temp.left != null) queue.offer(temp.left);
                if(temp.right != null) queue.offer(temp.right);
            }
        }
        // 返回列表
        return res;
    }
}
