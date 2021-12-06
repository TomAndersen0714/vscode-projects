package nowcoder;
    // 输入一个整数数组，判断该数组是不是某二叉搜索树的后序遍历的结果。
    // 如果是则输出Yes,否则输出No。假设输入的数组的任意两个数字都互不相同。
public class Nowcoder23{
    // 方法1:递归
    // 每次取出后续数组中的最后一个数,此数必定为二叉搜索树中的根节点
    // 在数组中找到第一个比根节点值大的数,然后继续遍历,若之后还存在比
    // 根节点值小的值,则表明此序列不可能为二叉搜索树的后序,如果没找到
    // 则将序列一分为二,然后继续递归判断子树是否也可能为二叉搜索树.
    // 注意:题目的要求是可能为某二叉搜索树,只要有可能就返回true
    public boolean VerifySquenceOfBST(int [] sequence) {
        // 排除特殊情况
        if(sequence == null || sequence.length == 0) return false;
        // 递归判断
        return VerifySquenceOfBST(sequence, 0, sequence.length - 1);
    }
    
    public boolean VerifySquenceOfBST(int[] sequence,int begin, int end){
        // 排除特殊情况,并设置递归出口
        // 当待查找序列长度小于2时,必定为某个二叉搜索树的后序,直接返回true
        if(end - begin <= 1) return true;
        // 遍历序列,从中找到第一个大于sequence[end]数的位置,并判断后续是否有小于此数的数
        // 若有则直接返回false,表明无法形成二叉搜索树,否则递归迭代左右子树
        int index = -1;
        for(int i=begin; i<=end; i++){
            if(sequence[i] >= sequence[end]) index = i;
            if(index!=-1 && sequence[i]<sequence[end]) return false;
        }
        return VerifySquenceOfBST(sequence,begin,index-1) &&
            VerifySquenceOfBST(sequence,index+1,end);
    }
}