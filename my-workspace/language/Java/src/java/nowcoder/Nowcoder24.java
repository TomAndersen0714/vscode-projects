package java.nowcoder;

import java.util.ArrayList;
import java.util.Collections;

// 输入一颗二叉树的根节点和一个整数，打印出二叉树中结点值的和为输入整数的所有路径。
// 路径定义为从树的根结点开始往下一直到叶结点所经过的结点形成一条路径。
// (注意: 在返回值的list中，数组长度大的数组靠前)
public class Nowcoder24 {
    // 方法1:DFS递归
    // 主要思想:递归寻找叶节点,每次向下递归时target减去当前值,当找到叶节点并且target=0时
    // 将当前路径拷贝,加入到返回列表中.当所有叶节点都遍历完成后,再将返回列表排序,最后返回
    // 如果最后不要求排序,则单个方法就可以解决,一般情况下是不会要求的,但本题增加了额外要求
    ArrayList<Integer> path = new ArrayList<>(); // 当前递归路径
    ArrayList<ArrayList<Integer>> paths = new ArrayList<>(); // 所有遍历路径

    public ArrayList<ArrayList<Integer>> FindPath(TreeNode root, int target) {
        // 排除特殊情况
        if (root == null)
            return paths;
        // 递归寻找路径集合
        findPathsToLeaf(root, target);
        // 对找到的路径集合进行降序排序(默认为升序)
        Collections.sort(paths, (o1, o2) -> {
            return o2.size() - o1.size();
        });
        // 返回路径集合
        return paths;
    }

    // 递归寻找到达叶节点路径值等于target的路径
    private void findPathsToLeaf(TreeNode root, int target) {
        // 排除特殊情况
        if (root == null)
            return;
        // 将当前节点加入递归查找路径中,并且更新target:target-=root.val
        path.add(root.val);
        target -= root.val;
        // 如果当前节点为叶节点,且target==0,则将当前递归路径加入到返回列表中
        if (root.left == null && root.right == null && target == 0) {
            paths.add(new ArrayList<>(path));
        } else {// 否则递归其左右子树
            findPathsToLeaf(root.left, target);
            findPathsToLeaf(root.right, target);
        }
        // 当前节点递归遍历结束,将当前节点从遍历路径中取出
        path.remove(path.size() - 1);
    }
}