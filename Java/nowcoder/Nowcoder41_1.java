package nowcoder;
// 输入正整数sum,输出和为sum的所有连续正数的序列(至少包含两个数)
// 要求:输出序列内按照从小至大的顺序,序列间按照开始数字从小到大的顺序
import java.util.ArrayList;
public class Nowcoder41_1{
    // 方法2:方法1的改良版
    // 思路:双重循环,不再像是方法1中使用公式计算,没必要,直接挨个进行加运算即可
    public ArrayList<ArrayList<Integer> > FindContinuousSequence(int sum) {
        // 创建返回列表
        ArrayList<ArrayList<Integer>> res = new ArrayList<>();
        // 排除特殊情况
        if(sum <= 0) return res;
        // 构建双重循环遍历所有可能
        for(int i=1;i<=sum/2;i++){
            int j=i,s = 0;
            while(s < sum){
                s += j;
                j++;
            }
            if(s == sum){// 如果当前序列和等于目标值,则将序列添加到返回列表中
                ArrayList<Integer> seq = new ArrayList<>();
                for(int k=i;k<j;k++){
                    seq.add(k);
                }
                res.add(seq);
            }
        }
        return res;
    }
}