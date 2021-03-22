package nowcoder;
// 输入一棵二叉搜索树，将该二叉搜索树转换成一个排序的双向链表。
// 要求不能创建任何新的结点，只能调整树中结点指针的指向。
public class Nowcoder26_1{
    // 方法2:递归
    // 每次递归时,保存当前双向链表的尾结点(即左子树的最右节点)
    // 而Convert本身的返回值为链表的头结点
    // 由此可以将当前节点链接到链表尾和链表头之间.
    TreeNode lastNode;// 保存链表的当前尾节点
    public TreeNode Convert(TreeNode root) {
        // 排除特殊情况
        if(root == null || (root.left == null && root.right == null)) return root;
        // 返回链表头节点
        return getListHead(root);
    }
    
    // 获取转换成双链表后的头结点
    private TreeNode getListHead(TreeNode root){
        // 定义左右子树头结点
        TreeNode leftHead = null, rightHead = null;
        // 如果左子树非空,则遍历左子树
        if(root.left != null) leftHead = getListHead(root.left);
        // 将当前节点left指针指向当前链表尾部节点
        root.left = lastNode;
        // 若当前链表尾部节点非null,则将其right指针指向当前节点
        if(lastNode != null) lastNode.right = root;
        // 更新当前链表的尾结点
        lastNode = root;
        // 如果右子树非空,则遍历右子树
        if(root.right != null) rightHead = getListHead(root.right);
        // 将当前节点的right指针指向右子树
        root.right = rightHead;
        // 若右子树头结点非null,则将其left指针指向当前节点
        if(rightHead != null) rightHead.left = root;
        // 返回左子树链表头结点,若为null则返回当前节点,否则直接返回左子树头结点
        return leftHead == null ? root:leftHead;
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