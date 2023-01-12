package nowcoder;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

// 本类专门用于处理TreeNode
public class TreeNodes {
    // 根据树前中序构造树
    public static TreeNode buildTree(int[] preOrder,int[] inOrder){
        // 排除特殊情况
        if(preOrder == null || inOrder == null || preOrder.length != inOrder.length)
            return null;
        // 创建树根节点
        TreeNode root = buildTree(preOrder,0,preOrder.length-1,inOrder,0,inOrder.length);
        return root;
    }

    // 根据树前中序的指定部分递归构造树
    private static TreeNode buildTree(int[] preOrder,int s1,int e1,int[] inOrder,int s2,int e2){
        if(s1<=e1 && s2<=e2){
            // 查找当前前序第一个节点在中序中的位置
            for(int i = 0; i<=e2-s2; i++){
                // 当找到对应位置后,将前后序分割,递归构造左右子树
                if(preOrder[s1] == inOrder[s2+i]){
                    // 创建当前树根
                    TreeNode root = new TreeNode(preOrder[s1]);
                    root.left = buildTree(preOrder,s1+1,s1+i,inOrder,s2,s2+i-1);
                    root.right = buildTree(preOrder,s1+i+1,e1,inOrder,s2+i+1,e2);
                    return root;
                }
            }
        }
        // 返回构造后的树
        return null;
    }

    // 树的层次遍历
    public static void levelTraversalTree(TreeNode root,List<Integer> list){
        // 排除特殊情况
        if(root == null || list == null) return;
        // 使用队列进行层次遍历
        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);
        // 进行层次遍历
        int size;
        TreeNode temp;
        while(!queue.isEmpty()){
            size = queue.size();
            while(size-->0){
                temp = queue.poll();
                list.add(temp.val);
                if(temp.left != null) queue.offer(temp.left);
                if(temp.right != null) queue.offer(temp.right);
            }
        }
    }

    // 树的中序打印(递归)
    public static void printTreeInOrder(TreeNode root){
        if(root == null) System.out.print("#");
        else{
            printTreeInOrder(root.left);
            System.out.print(root.val);
            printTreeInOrder(root.right);
        }
    }

    // 数的前序打印(递归)
    public static void printTreePreOrder(TreeNode root){
        if(root == null) System.out.print("#");
        else{
            System.out.print(root.val);
            printTreePreOrder(root.left);
            printTreePreOrder(root.right);
        }
    }
}

class TreeNode {
    int val = 0;
    TreeNode left = null;
    TreeNode right = null;

    public TreeNode(int val) {
        this.val = val;

    }
}