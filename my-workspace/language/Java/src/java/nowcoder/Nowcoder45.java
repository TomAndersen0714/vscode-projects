package java.nowcoder;
import java.util.TreeSet;
// 从扑克牌中随机抽取5张牌,判断是不是一个顺子,即这5张牌是不是连续的.
// 2~10为数字本身,1代表A,11/12/13分别代表J/Q/K,0代表大小王,而大小王
// 可以代表任何数字.
public class Nowcoder45{
    // 方法1:使用TreeSet辅助实现(PS:TreeSet是默认升序的Set,是个不包含重复元素的有序数据结构)
    // 思路:遍历数组,使用count统计0的个数,并将非0的数字存入TreeSet中,Set结构不会存放重复数字.
    // 遍历结束后,如果TreeSet.size()+count不等于5,则表明输入数中存在重复数字,直接返回false.
    // 如果TreeSet中的最大最小数之差大于4,则表明0的个数无法使得输入数组为顺序,直接返回false.
    // 最终返回true.
    // 时间复杂度:O(n),空间复杂度:O(n)
    public boolean isContinuous(int [] numbers) {
        // 排除特殊情况
        if(numbers == null || numbers.length != 5) return false;
        // 创建辅助数据结构
        TreeSet<Integer> treeSet  = new TreeSet<>();
        int count = 0;// 统计0的个数
        // 遍历输入数组
        for(int num:numbers){
            if(num == 0) count++;
            else treeSet.add(num);
        }
        // 如果存在重复数字,则返回false
        if((treeSet.size()+count) != 5) return false;
        // 如果0的个数不够凑成顺子,则返回false
        if((treeSet.last()-treeSet.first()) > 4) return false;
        return true;
    }
}