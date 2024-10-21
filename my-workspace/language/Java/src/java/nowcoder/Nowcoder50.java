package java.nowcoder;
// 在一个长度为n的数组里的所有数字都在0到n-1的范围内。 
// 数组中某些数字是重复的，但不知道有几个数字是重复的。也不知道每个数字重复几次。
// 请找出数组中任意一个重复的数字。 
// 例如，如果输入长度为7的数组{2,3,1,0,2,5,3}，那么对应的输出是第一个重复的数字2。
import java.util.Arrays;
public class Nowcoder50{
    // 方法1:排序+遍历
    // 思路:先对输入数组进行排序,然后遍历数组,判断是否存在相邻的重复数字,如果存在
    // 则将此数字添加到输出数组中,然后直接返回true.遍历结束时,返回false,表明没有重
    // 复元素.
    // 时间复杂度:O(n*logn),空间复杂度:O(1)
    public boolean duplicate(int numbers[],int length,int [] duplication) {
        // 排除特殊情况
        if(numbers == null || numbers.length != length || numbers.length <=1)
            return false;
        // 对数组排序
        Arrays.sort(numbers);
        // 遍历数组,获取重复元素
        for(int i=1; i<length; i++){
            if(numbers[i-1] == numbers[i]){
                duplication[0] = numbers[i];
                return true;
            }
        }
        // 遍历结束,则表示没有重复元素,直接返回false
        return false;
    }
}