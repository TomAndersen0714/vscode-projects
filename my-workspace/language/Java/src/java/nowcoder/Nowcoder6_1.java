package java.nowcoder;
//一只青蛙一次可以跳上1级台阶，也可以跳上2级。
//求该青蛙跳上一个n级的台阶总共有多少种跳法（先后次序不同算不同的结果）。
public class Nowcoder6_1{
	// 使用矩阵保存迭代的子结果,避免重复计算
	public int JumpFloor(int target) {
        // 排除特殊情况
        if(target == 0) return 0;
        if(target == 1) return 1;
        if(target == 2) return 2;
        int[] array = new int[target+1];
        array[1] = 1;
        array[2] = 2;
        return JumpFloor(array,target);
    }
    
    private int JumpFloor(int[] array,int target){
        if(target <= 0) return 0;
        if(array[target] == 0){
            return JumpFloor(array,target-1)+JumpFloor(array,target-2);
        }else return array[target];
    }
}