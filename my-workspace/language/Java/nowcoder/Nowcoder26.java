package nowcoder;
import java.util.ArrayList;
// 输入一棵二叉搜索树，将该二叉搜索树转换成一个排序的双向链表。
// 要求不能创建任何新的结点，只能调整树中结点指针的指向。
public class Nowcoder26{
    // 方法1:Brute Force
    // 先递归获取中序,将节点依次加入到容器中,最后遍历容器
    // 将其中的节点链接成双向链表
    private ArrayList<TreeNode> inOrder = new ArrayList<>();
    public TreeNode Convert(TreeNode root) {
        // 排除特殊情况
        if(root == null || (root.left == null && root.right == null)) return root;
        // 获取中序
        getInOrder(root);
        // 先处理首尾节点
        int len = inOrder.size();
        inOrder.get(0).left =null;
        inOrder.get(0).right = inOrder.get(1);
        inOrder.get(len-1).left = inOrder.get(len-2);
        inOrder.get(len-1).right = null;
        // 遍历容器
        for(int i=1; i<len-1; i++){
            inOrder.get(i).left = inOrder.get(i-1);
            inOrder.get(i).right = inOrder.get(i+1);
        }
        // 返回头结点
        return inOrder.get(0);
    }
    
    // 获取中序遍历
    private void getInOrder(TreeNode root){
        // 排除特殊情况,同时设置递归出口
        if(root == null) return;
        getInOrder(root.left);
        inOrder.add(root);
        getInOrder(root.right);
    }
}
/*
class TreeNode {
    int val = 0;
    TreeNode left = null;
    TreeNode right = null;

    public TreeNode(int val) {
        this.val = val;

    }

}
*/