package java.nowcoder;
//输入某二叉树的前序遍历和中序遍历的结果，请重建出该二叉树。
//假设输入的前序遍历和中序遍历的结果中都不含重复的数字。
//例如输入前序遍历序列{1,2,4,7,3,5,6,8}和中序遍历序列{4,7,2,1,5,3,8,6}，则重建二叉树并返回。
public class Nowcoder4{
	public TreeNode reConstructBinaryTree(int [] pre,int [] in) {
        //排除特殊情况
        if(pre == null || in == null) return null;
        return reConstructBinaryTree(pre,0,pre.length-1,in,0,in.length-1);
        
    }
    private TreeNode reConstructBinaryTree(int[] pre,int s1,int e1,
                                           int[] in,int s2,int e2){
        if(s1>e1 || s2>e2) return null;
        TreeNode root = new TreeNode(pre[s1]);
        // 定位
        for(int i = 0; i <= e2-s2; i++){
            if(in[s2+i] == pre[s1]){
                root.left = reConstructBinaryTree(pre,s1+1,s1+i,in,s2,s2+i-1);
                root.right = reConstructBinaryTree(pre,s1+i+1,e1,in,s2+i+1,e2);
                break;
            }
        }
        return root;
    }
}