package java.nowcoder;
// 数组中有一个数字出现的次数超过数组长度的一半，请找出这个数字。
// 例如输入一个长度为9的数组{1,2,3,2,2,2,5,4,2}。由于数字2在数组中出现了5次，
// 超过数组长度的一半，因此输出2。如果不存在则输出0。
public class Nowcoder28_2{
    // 方法3:先遍历数组,获取可能超过半数的数字值,然后第二次遍历数组,统计对应值
    // 出现的次数,若出现次数大于数组长度的一半,则将其返回,否则返回0.
    // 获取出现次数可能超过半数的数字的方式:设置变量preValue保存一个从首次出现开始到目前位置
    // 出现次数大于这段长度的1/2的值,并设置变量count记录这段距离preValue出现次数-其他数字次数的值
    // 每次遇到等于preValue则count++,否则count--,若count==0,则将preValue换成当前值,然后继续遍历.
    // 时间复杂度:O(n)
    public int MoreThanHalfNum_Solution(int [] array) {
        // 排除特殊情况
        if(array == null || array.length == 0) return 0;
        // 统计首次出现位置到结尾出现次数大于这段距离的1/2的数字
        int preValue = array[0],count = 1;
        for(int i=1; i<array.length; i++){
            if(array[i] == preValue) count++;
            else{
                count--;
                if(count == 0){
                    preValue = array[i];
                    count =1;
                }
            }
        }
        // 统计preValue出现的次数
        count = 0;
        for(int num:array){
            if(num == preValue) count++;
        }
        return count>array.length/2?preValue:0;
    }
}