package java.nowcoder;
import java.util.Arrays;
// 数组中有一个数字出现的次数超过数组长度的一半，请找出这个数字。
// 例如输入一个长度为9的数组{1,2,3,2,2,2,5,4,2}。由于数字2在数组中出现了5次，
// 超过数组长度的一半，因此输出2。如果不存在则输出0。
public class Nowcoder28_1{
    // 方法2:先对数组进行排序,然后设置统计变量,顺序遍历数组
    // 若当前数与前一个数相同,则将统计变量+1,否则统计变量置0
    // 当统计变量大于数组长度的一半时,将其此数字返回,否则继续
    // 遍历
    // 时间复杂度:O(nlogn)
    public int MoreThanHalfNum_Solution(int [] array) {
        // 排除特殊情况
        if(array == null || array.length == 0) return 0;
        if(array.length == 1) return array[0];
        // 排序数组,并创建统计变量
        Arrays.sort(array);
        int count = 1,threshold = array.length/2;
        // 遍历数组
        for(int i=1; i<array.length; i++){
            if(array[i] == array[i-1]) count++;
            else count=1;
            if(count > threshold) return array[i];
        }
        // 如没有数字超过数组长度一半,则返回0
        return 0;
    }
}