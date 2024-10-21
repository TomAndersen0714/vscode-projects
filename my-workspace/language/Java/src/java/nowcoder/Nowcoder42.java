package java.nowcoder;
import java.util.ArrayList;
// 输入一个递增排序的数组和一个数字S，在数组中查找两个数，使得他们的和正好是S，
// 如果有多对数字的和等于S，输出两个数的乘积最小的。
// 注意:对应每个测试案例，输出两个数，小的先输出。
import java.util.HashSet;
public class Nowcoder42{
    // 方法1:使用HashSet辅助
    // 思路:遍历输入数组,每次在HashSet中查找是否存在sum-array[i]的值,如果存在,则此组合
    // 可能为目标值,故尝试更新返回值.如果不存在,则将当前值添加到HashSet中,继续遍历,直
    // 到遍历结束.
    // 时间复杂度:O(n*logn),空间复杂度:O(n)
    public ArrayList<Integer> FindNumbersWithSum(int [] array,int sum) {
        // 创建返回列表
        ArrayList<Integer> res = new ArrayList<>();
        // 排除特殊情况
        if(array == null || array.length == 0) return res;
        // 创建变量保存可能的返回值
        int a = 0,b = 0;// 保存返回变量
        long temp = Long.MAX_VALUE;// 保存a*b的值
        // 创建辅助HashSet
        HashSet<Integer> hashSet = new HashSet<>();
        // 遍历数组
        for(int num:array){
            if(hashSet.contains(sum-num)){
                if(num*(sum-num) <= temp){
                    temp = num*(sum-num);
                    a = sum-num;
                    b = num;
                }
            }else hashSet.add(num);
        }
        // 如果变量更新,则将返回值添加到返回列表中
        if(temp!=Long.MAX_VALUE || (a!=0 && b!=0)){
            res.add(a);
            res.add(b);
        }
        return res;
    }
}