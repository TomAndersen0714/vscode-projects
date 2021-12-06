package nowcoder;
public class Nowcoder11{
	// 输入一个整数，输出该数二进制表示中1的个数。其中负数用补码表示。
	public int NumberOf1(int n) {
		int count=0;
		while(n!=0){
			if((n&1)==1) count++;
			// 无符号右移,默认左侧添加0
			n = n>>>1;
		}
		return count;
	}
}