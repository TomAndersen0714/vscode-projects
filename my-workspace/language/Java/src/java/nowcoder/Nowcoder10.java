package java.nowcoder;
// 我们可以用2*1的小矩形横着或者竖着去覆盖更大的矩形。
// 请问用n个2*1的小矩形无重叠地覆盖一个2*n的大矩形，总共有多少种方法？
// 比如n=3时，2*3的矩形块有3种覆盖方法：
public class Nowcoder10{
	public int RectCover(int target) {
		// 排除特殊情况
		if(target<=0) return 0;
		if(target==1) return 1;
		if(target==2) return 2;
		if(target==3) return 3;
		// 使用数组保存子集结果
		int[] array = new int[target+1];
		array[0] = 1;
		array[1] = 1;
		array[2] = 2;
		array[3] = 3;
		for(int i=4; i<=target; i++){
			array[i]+=(array[i-1]+array[i-2]);
		}
		return array[target];
	}
}