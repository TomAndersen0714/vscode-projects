package java.nowcoder;
// 输入正整数sum,输出和为sum的所有连续正数的序列(至少包含两个数)
// 要求:输出序列内按照从小至大的顺序,序列间按照开始数字从小到大的顺序
import java.util.ArrayList;
public class Nowcoder41_2{
    // 方法3:双指针(滑动窗口)
    // 思路:使用双指针构建滑动窗口的思想,使用left/right分别表示窗口两侧的值,窗口中存放的是
    // 连续正数,使用win表示窗口中数字之和,初始时left=1,right=2.
    // 当win<sum时,向右扩张窗口,right++,win+=right;
    // 当win>sum时,向右减小窗口,win-=left,left++;
    // 当win=sum时,则将left~right之间的数都放入新建序列中,并添加到返回列表中,然后缩小窗口
    // 即win-=left,left++
    // 直到left=sum/2时退出循环,表示已经遍历完所有可能序列
    // 通过构建窗口序列二叉树,可以得知,这种遍历策略确实能够遍历所有可能序列.
    // 时间复杂度:O(n),空间复杂度:O(1)
    public ArrayList<ArrayList<Integer> > FindContinuousSequence(int sum) {
        // 创建返回列表
        ArrayList<ArrayList<Integer>> res = new ArrayList<>();
        // 排除特殊情况(当sum<=2时,没连续正数序列之和等于sum)
        if(sum <= 2) return res;
        // 规定初始窗口大小
        int left = 1,right = 2,win = 3;
        // 遍历所有可能序列
        while(left <= sum/2){
            if(win < sum){
                right ++;
                win += right;
            }
            else if(win > sum){
                win -= left;
                left++;
            }
            else{
                ArrayList<Integer> seq = new ArrayList<>();
                for(int i=left;i<=right;i++){
                    seq.add(i);
                }
                res.add(seq);
                win-=left;
                left++;
            }
        }
        // 返回结果
        return res;
    }
}