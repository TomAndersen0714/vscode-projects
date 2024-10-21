package java.nowcoder;
// 在一个长度为n的数组里的所有数字都在0到n-1的范围内。 
// 数组中某些数字是重复的，但不知道有几个数字是重复的。也不知道每个数字重复几次。
// 请找出数组中任意一个重复的数字。 
// 例如，如果输入长度为7的数组{2,3,1,0,2,5,3}，那么对应的输出是第一个重复的数字2。
import java.util.HashSet;
public class Nowcoder50_1{
    // 方法2:使用HashSet辅助完成
    // 思路:遍历输入数组,每次遍历时,判断HashSet中是否已经存在相同元素,如果不存在
    // 则将当前元素添加到Set中,如果存在则直接将当前数字添加到指定数组中,然后返回true
    // 遍历结束时,直接返回false,表明没有重复数字.此思路主要利用了HashMap中get方法
    // /containsKey方法的时间复杂度为O(1)的性质来实现.
    // 时间复杂度:O(n),空间复杂度:O(n)
    public boolean duplicate(int numbers[],int length,int [] duplication) {
        // 排除特殊情况
        if(numbers == null || numbers.length != length || numbers.length <= 1)
            return false;
        // 定义辅助变量
        HashSet<Integer> hashSet  = new HashSet<>();
        // 遍历数组
        for(int num:numbers){
            if(hashSet.contains(num)){
                duplication[0] = num;
                return true;
            }else hashSet.add(num);
        }
        // 遍历结束表明无重复元素
        return false;
    }
}