package java.nowcoder;
import java.util.Arrays;
// 一个整型数组里除了两个数字之外，其他的数字都出现了两次。请写程序找出这两个只出现一次的数字。
// num1,num2分别为长度为1的数组。传出参数
// 将num1[0],num2[0]设置为返回结果
public class Nowcoder40_1{
	// 方法2:排序+遍历
	// 思路:先将原数组进行升序排序(Arrays.sort),然后使用双指针指向数组开头的两个相邻
	// 元素,对数组进行遍历.如果双指针指向的元素相同,则将双指针+2;如果双指针指向的元素
	// 不同,则将array[left]的值保存在对应位置,如此遍历直到数组遍历结束.在保存元素时,使
	// 用访问标记变量first来标记,是否已经找到了第一个元素,如果找到了则将下一个保存在第
	// 二位置,否则保存在第一个位置.
	// 时间复杂度:O(nlogn)
	public void FindNumsAppearOnce(int [] array,int num1[] , int num2[]) {
		// 排除特殊情况
		if(array == null || array.length <= 1) return;
		// 对原数组进行升序排序
		Arrays.sort(array);
		// 使用双指针指向两个相邻元素
		int left = 0,right = 1;
		// 访问标记变量标记首个元素是否已经找到
		boolean first = false;
		// 遍历数组
		while(right <= array.length - 1){
			// 如果两个指针指向的元素相同,则同时+2
			if(array[left] == array[right]){
				left += 2;
				right += 2;
			}else{
				// 如果首个元素还未找到,则将本次找到的元素放置在首个位置,并变更访问标记变量
				if(!first){
					num1[0] = array[left];
					first = true;
					left++;
					right++;
				}else{
					num2[0] = array[left];
					return;
				}
			}
		}
		// 如果right越界也没找到第二个元素,则表明最后一个元素必定在数组结尾
		num2[0] = array[left];
	}
}