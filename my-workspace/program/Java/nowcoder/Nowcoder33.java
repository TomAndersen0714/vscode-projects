package nowcoder;
// 丑数
// 把只包含质因子2、3和5的数称作丑数（Ugly Number）。例如6、8都是丑数，但14不是，因为它包含质因子7。
// 习惯上我们把1当做是第一个丑数。求按从小到大的顺序的第N个丑数（若N小于1,则返回0）。
public class Nowcoder33{
    // 方法1:使用三指针
    // 思路:三指针分别代表三种寻找下一个丑数的方式,即乘以2/3/5,三指针一开始分别
    // 指向第一个丑数,每次选取三指针指向的值乘以2/3/5的最小值作为下一个丑数,若
    // 三指针中某个指针指向的丑数乘以对应的因子等于最新的丑数,则将对应的指针后移,
    // 遍历n次,直到找到第n个丑数
    public int GetUglyNumber_Solution(int n) {
        // 排除特殊情况
        if(n<1) return 0;
        // 创建数组保存已经找到的丑数,提供三指针一个查找方向
        int[] ugly = new int[n];
        // 初始化第一个丑数
        ugly[0] = 1;
        // 定义三指针
        int str_2=0,str_3=0,str_5=0;
        // 遍历n次,每次取下一批最可能的三个丑数中的最小值作为下一个丑数
        for(int i=1; i<n; i++){
            // 确定下一个丑数
            ugly[i] = Math.min(ugly[str_2]*2,Math.min(ugly[str_3]*3,ugly[str_5]*5));
            // 若某个指针指向的丑数可以产生新的丑数,则将其指针后移
            if(ugly[i]==ugly[str_2]*2) str_2++;
            if(ugly[i]==ugly[str_3]*3) str_3++;
            if(ugly[i]==ugly[str_5]*5) str_5++;
        }
        // 返回对应的丑数
        return ugly[n-1];
    }
}