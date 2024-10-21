package java.nowcoder;
// 约瑟夫环问题:
// 0~n-1这n个数字排成一个圆圈,从数字0开始每次从这个圆圈里删除第m个数字,
// 然后从删除位置的下一个位置开始计数,继续删除第m个数字.
// 求出这个圆圈里剩下的最后一个数字.
// PS:如果不存在此数字,则返回-1
// PS:不要想着自定义额外的数据结构(如:链表节点),只能使用已有的数据结构.
import java.util.ArrayList;
public class Nowcoder46{
    // 方法1:使用ArrayList充当链表模拟解题过程
    // 思路:创建ArrayList将0~n-1填充进List中,使用指针cursor指向下一个待删除
    // 元素的索引,初始时cursor=0,迭代n-1次,每次迭代时cursor=(cursor+m-1)%size
    // 然后删除列表中cursor对应的元素,继续迭代,一直迭代n-1次为止.最后返回列表
    // 中的最后一个元素.
    // 时间复杂度:O(n*n),空间复杂度:O(n)
    public int LastRemaining_Solution(int n, int m) {
        // 排除特殊情况
        if(n <= 0 || m <= 0) return -1;
        // 创建ArrayList将0~n-1填充进List中
        ArrayList<Integer> arrayList = new ArrayList<>();
        for(int i=0;i<n;i++) arrayList.add(i);
        // 迭代删除cursor元素,删除n-1次
        int cursor = 0;
        for(int size=n;size>1;size--){
            cursor = (cursor + m - 1)%size;
            arrayList.remove(cursor);
        }
        // 返回最后一个元素
        return arrayList.get(0);
    }
}