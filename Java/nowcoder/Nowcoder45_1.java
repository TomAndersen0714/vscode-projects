package nowcoder;
// 从扑克牌中随机抽取5张牌,判断是不是一个顺子,即这5张牌是不是连续的.
// 2~10为数字本身,1代表A,11/12/13分别代表J/Q/K,0代表大小王,而大小王
// 可以代表任何数字.
import java.util.Arrays;
public class Nowcoder45_1{
    // 方法2:使用Arrays.sort(int [] array)升序排序
    // 思路:排除特殊情况后,对输入数组进行排序,使用cursor指向数组中最小非0数字位置,
    // 初始时为0.然后遍历输入数组,如果遇到0,则cursor后移,否则,判断当前数字是否和下一
    // 个数字相同,如果相同,则直接返回false,直到遍历结束.
    // 最终判断输入数组中的最大值和最小非0值的差值,是否小于5
    public boolean isContinuous(int [] numbers) {
        // 排除特殊情况
        if(numbers == null || numbers.length != 5) return false;
        // 对输入数组进行升序排序
        Arrays.sort(numbers);
        // 遍历排序后数组
        int cursor = 0; // 指向最小非0元素,同时代表0的个数
        int length = numbers.length;
        for(int i=0;i<length;i++){
            if(numbers[i] == 0) cursor++;
            else if(i+1<length && numbers[i]==numbers[i+1]) return false;
        }
        // 若最终0的个数满足形成顺子(即),则返回true,否则返回false
        return numbers[length-1]-numbers[cursor] <= 4;
    }
}