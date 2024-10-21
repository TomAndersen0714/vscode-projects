package java.nowcoder;
// 一个整型数组里除了两个数字之外，其他的数字都出现了两次。请写程序找出这两个只出现一次的数字。
// num1,num2分别为长度为1的数组。传出参数
// 将num1[0],num2[0]设置为返回结果
public class Nowcoder40_2{
	// 方法3:使用异或运算的思想
	// 异或运算的性质:a^b = b^a,a^0 = a,a^a=0
	// 思路:将数组中的所有元素进行异或求和,设求得的和值为xorSum,取xorSum的最后一个
	// 为1的bit位,将其他位设置为0,则此bit位代表着两个仅出现1次的值之间的差别.
	// 根据此bit位的值,将整个数组中的值分成两部分,则两个仅出现1次的值必定在两个不
	// 同的集合中,而两个集合中其他的数字也必定出现了2次,因此又可以通过异或的方式来
	// 排除出现2次的数组,最终可以得到两个仅出现1次数字.但此方法并不是此类问题的通用
	// 解法.
	// 首先遍历数组,进行异或求和,设求得的值为xorSum,取xorSum的最后一个为1的bit为,将
	// 其他的bit为都置为1,将此值保存为rightFirstOneBit.再次遍历原数组,使用rightFirstOneBit
	// 值与每一个元素按位与运算,将与运算结果等于rightFirstOneBit的进行异或求和,遍历结束后
	// 则获取第一个只出现了1次的数字,然后将其与xorSum进行异或运算,则求得第二个只出现1次的数字
	// 因为xorSum^a=a^b^a=b
	// 时间复杂度:O(n)
	public void FindNumsAppearOnce(int [] array,int num1[] , int num2[]) {
		// 排除特殊情况
		if(array == null || array.length <=1) return;
		// 遍历数组,进行异或求和
		int xorSum = 0;
		for(int num:array){
			xorSum ^= num;
		}
		// 获取首个为1的bit,并保存其值
		int rightFirstOneBit = xorSum & (~xorSum + 1);
		// 再次遍历元数组,将与rightFirstOneBit按位与运算结果为0的进行异或求和
		// 最终结果即为其中某个仅出现1次的数字
		int a = 0;
		for(int num:array){
			if((num & rightFirstOneBit) == 0) a^=num;
		}
		// 求得a后很容易求得b
		int b = xorSum ^ a;
		// 将两个数字放置在指定位置后返回
		num1[0] = a;
		num2[0] = b;
	}
}