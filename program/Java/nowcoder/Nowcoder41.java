package nowcoder;
// 输入正整数sum,输出和为sum的所有连续正数的序列(至少包含两个数)
// 要求:输出序列内按照从小至大的顺序,序列间按照开始数字从小到大的顺序
import java.util.ArrayList;
public class Nowcoder41{
    // 方法1:Brute Force
    // 思路:使用二重循环,从数字i开始计算n个连续数字的和,初始时i=1,n=2,
    // 从i开始的连续n个数字的和为s = (i+i+n-1)*n/2 = n*i+n*(n-1)/2
    // 循环条件为for(int i=1;i<=sum/2;i++);for(int n=2;;n++)
    // 若s=sum,则将当前序列添加到返回列表中,跳出当前循环
    // 若s<sum,则继续循环
    // 若s>sum,则直接跳出循环
    // 时间复杂度:O(n*n)
    public ArrayList<ArrayList<Integer> > FindContinuousSequence(int sum) {
        // 创建返回列表
        ArrayList<ArrayList<Integer>> res = new ArrayList<>();
        // 排除特殊情况
        if(sum<=0) return res;
        // 构建双重循环,遍历所有可能
        int s = 0;
        for(int i=1;i<=sum/2;i++){
            for(int n=2;;n++){
                s = n*i+n*(n-1)/2;
                if(s == sum){
                    // 创建序列
                    ArrayList<Integer> seq = new ArrayList<>();
                    for(int j=i;j<=i+n-1;j++){
                        seq.add(j);
                    }
                    res.add(seq);
                    break;
                }
                else if(s>sum) break;
            }
        }
        return res;
    }
}