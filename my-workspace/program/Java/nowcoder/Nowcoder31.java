package nowcoder;
// 求出1~13的整数中1出现的次数,并算出100~1300的整数中1出现的次数
// 如n=13时,1~13中包含1的数字有1、10、11、12、13因此数字1共出现6次
public class Nowcoder31{
    // 方法1:将n上各个数位的值视为可以在0~9范围内随意变换,原题就变成了
    // 寻找组合值小于n的组合,统计各个数位上出现1的组合个数.将n上各个数
    // 位分别设置为1,统计不同情况下可能的组合个数,最后对组合个数求和.
    // 具体解析参考:https://blog.csdn.net/TomAndersen/article/details/105754020
    public int NumberOf1Between1AndN_Solution(int n) {
        // 排除特殊情况
        if(n < 0) return 0;
        // 定义左侧数,当前数,右侧数
        int leftNum,curNum,rightNum;
        // 定义当前数位,数字1出现的次数
        int k=1, count=0;
        // 遍历n的各个数位,统计各个数位上为1的可能情况
        while(k <= n){
            leftNum = n/k/10;
            curNum = n/k%10;
            rightNum = n%k;
            if(curNum == 1) count += leftNum*k+rightNum+1;
            else if(curNum == 0) count += leftNum*k;
            else count += (leftNum+1)*k;
            k*=10;
        }
        // 返回统计值
        return count;
    }
}