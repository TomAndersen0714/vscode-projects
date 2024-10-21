package java.nowcoder;
// 连续子序列的最大值
// 例如:{6,-3,-2,7,-15,1,2,2},连续子向量的最大和为8(从第0个开始,到第3个为止)。
// 给一个数组，返回它的最大连续子序列的和(子向量的长度至少是1)
public class Nowcoder30{
    // 方法1:动态规划
    // 原问题(最大连续子序列的和)等价于求解以各个元素结尾的连续子序列之和的最大值
    // 创建DP数组保存子集的解,其中dp[i]表示以array[i]结尾的子序列中和的最大值
    // 于是便有递推公式dp[i]=max{dp[i-1]+array[i],array[i]},其中dp[0]=array[0]
    // 遍历数组求解dp数组,同时保存dp数组的最大值,遍历结束时返回此最大值即可
    // 求解动态规划问题的核心：①原问题具有重叠子结构；②从子问题到原问题的递推公式
    public int FindGreatestSumOfSubArray(int[] array) {
        // 排除特殊情况
        if(array == null || array.length == 0) return 0;
        // 创建DP数组,并设置临界条件
        int[] dp = new int[array.length];
        int max = array[0];
        dp[0] = array[0];
        // 遍历数组,求解dp数组
        for(int i=1; i<array.length; i++){
            // 若dp[i-1]小于0,则以array[i]结尾的子序列之和最大值必定为array[i]本身
            // 即dp[i-1]小于0时,dp[i]=array[i];
            dp[i]=dp[i-1]<=0?array[i]:(dp[i-1]+array[i]);
            // 记录当前dp数组中的最大值
            max = Math.max(max,dp[i]);
        }
        // 返回dp数组中的最大值
        return max;
    }
}