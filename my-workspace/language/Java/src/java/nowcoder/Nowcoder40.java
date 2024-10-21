package java.nowcoder;
import java.util.HashMap;
import java.util.Map;
// 一个整型数组里除了两个数字之外，其他的数字都出现了两次。请写程序找出这两个只出现一次的数字。
// num1,num2分别为长度为1的数组。传出参数
// 将num1[0],num2[0]设置为返回结果
public class Nowcoder40{
	// 方法1:使用HashMap统计词频
	// 思路:首先遍历原始数组,使用HashMap统计各个数字的出现次数,然后遍历HashMap的entrySet
	// 使用boolean变量标记是否已经找到了第一个数,若entrySet的value为1,若第一个数未找到,则
	// 将当前的key放入第一个数的对应位置,并将标记变量置为true,若第一个数已经找到即标记变量
	// 为true,则将当前的key放入第二个数的对应位置,并return
	// 时间复杂度:O(nlogn)
	public void FindNumsAppearOnce(int [] array,int num1[] , int num2[]) {
		// 排除特殊情况
		if(array == null || array.length <=1) return;
		// 创建HashMap用于统计词频
		HashMap<Integer,Integer> map = new HashMap<>();
		for(int num:array){
			map.put(num,map.getOrDefault(num,0)+1);
		}
		// 遍历HashMap的entrySet(),若value为1,则将其对应的key保存
		boolean first = false;
		for(Map.Entry<Integer,Integer> entry:map.entrySet()){
			if(entry.getValue() == 1){
				if(!first){
					num1[0] = entry.getKey();
					first = true;
				}else{
					num2[0] = entry.getKey();
					return;
				}
			}
		}
	}
}