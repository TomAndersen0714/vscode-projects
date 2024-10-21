package java.nowcoder;
public class Nowcoder9{
	// 一只青蛙一次可以跳上1级台阶，也可以跳上2级……它也可以跳上n级。
	// 求该青蛙跳上一个n级的台阶总共有多少种跳法。
	public int JumpFloorII(int target){
		// 排除特殊情况
		if(target == 0) return 1;
		if(target == 1) return 1;
		if(target == 2) return 2;
		// 使用数组保存子集结果
		int[] array = new int[target+1];
		array[0] = 1;
		array[1] = 1;
		array[2] = 2;
		for(int i=3; i<=target; i++){
			for(int j=i-1; j>=0; j--){
				array[i]+=array[j];
			}
		}
		return array[target];
	}
}