package java.nowcoder;
import java.util.HashMap;
// 数组中有一个数字出现的次数超过数组长度的一半，请找出这个数字。
// 例如输入一个长度为9的数组{1,2,3,2,2,2,5,4,2}。由于数字2在数组中出现了5次，
// 超过数组长度的一半，因此输出2。如果不存在则输出0。
public class Nowcoder28{
    // 方法1:使用容器统计词频,每次获取到一个数字则获取之前出现的次数
    // 然后将其+1,若大于数组长度的一半,则将其返回,否则重新压入map中,
    // 继续统计下一个数字
    // 时间复杂度:O(nlogn)
    public int MoreThanHalfNum_Solution(int [] array) {
        // 排除特殊情况
        if(array == null || array.length == 0) return 0;
        // 创建HashMap统计词频
        HashMap<Integer,Integer> map = new HashMap<>();
        // 遍历数组,统计各个数字的出现次数
        int count,threshold = array.length/2;
        for(int num:array){
            count = map.getOrDefault(num,0);
            count++;
            if(count > threshold) return num;
            map.put(num,count);
        }
        // 若没有出现次数超过一半的数字,则返回0
        return 0;
    }
}