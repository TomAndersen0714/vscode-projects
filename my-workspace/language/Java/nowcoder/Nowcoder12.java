package nowcoder;
public class Nowcoder12{
	// 给定一个double类型的浮点数base和int类型的整数exponent。求base的exponent次方。
	// 保证base和exponent不同时为0
	public double Power(double base, int exponent) {
		// 排除特殊情况
		if(exponent == 0) return 1;
		// 一般情况
		double res = 1.0;
		int i = exponent>0?exponent:(-exponent);
		while(i-->0) res*=base;
		return exponent>0?res:1/res;
	}
}