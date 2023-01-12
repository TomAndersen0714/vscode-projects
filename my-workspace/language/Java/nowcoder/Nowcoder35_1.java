package nowcoder;
import java.util.Arrays;
// 在数组中的两个数字，如果前面一个数字大于后面的数字，则这两个数字组成一个逆序对。
// 输入一个数组,求出这个数组中的逆序对的总数P。并将P对1000000007取模的结果输出。 即输出P%1000000007
// 数组中没有重复数字
public class Nowcoder35_1{
    // 方法2:分治(递归)+归并排序
	// 思路:将数组一分为二,假设逆序对中的两个数分别为a和b,则组成逆序对可以分为三种情况
	// 即ab都在数组左子数组中,ab都在右子数组中,ab分别在左右子数组中.对于前两者情况,可以
	// 看做是子问题求解,可以通过递归解得,因此每次递归时主要解决的就是第三种情况的逆序对
	// 个数.对于第三种情况,很容易想到使用二重循环来求解,但是时间复杂度为O(n^2),这样就显
	// 得本末倒置,因此需要采取更合适的方式来进行求解.此处一个比较合适的方式是使用归并排
	// 序,在保证左右子数组都分别有序的情况下,一边进行归并,一边统计第三种情况下的逆序对个
	// 数,这就需要降序归并.在归并的同时,若左子数组首个元素,大于右子数组的首个元素
	// 则右子数组剩余元素都能与左子数组的首个元素组成逆序对,因此需要统计右子数组剩余元素个数;
	// 而当左子数组的首个元素大于右子数组首个元素时,则正常归并即可.
	// 最后返回三种情况的逆序对个数,在统计之前需要保证三者之不超过int范围上限2^31-1.
	// 时间复杂度为:O(nlogn),空间复杂度:O(n)
	// 注意:由于逆序对个数可能超过int的上限,因此在求和之前需要先取余%1000000007
    public int InversePairs(int [] array) {
        // 排除特殊情况
		if(array == null || array.length <= 1) return 0;
		// 拷贝原数组,生成对应的merge数组
		int[] copy = Arrays.copyOf(array,array.length);
		// 求解并返回逆序对个数
		return divideAndMerge(array,copy,0,array.length-1);
    }
	
	// 分治(递归)+归并排序
	private int divideAndMerge(int[] array,int[] merge,int begin,int end){
		// 排除特殊情况,同时设置递归出口1
		if(end - begin < 1) return 0;
		// 将数组均分,分别递归左右子数组,对左右子数组进行排序并获取逆序对个数
		// 递归结束时,归并数组是局部有序的,因此在进行向下递归时需要将数组在参
		// 数列表中的位置交换保证后者有序
		int mid = begin + (end - begin)/2;
		int left = divideAndMerge(merge,array,begin,mid);
		int right = divideAndMerge(merge,array,mid+1,end);
		// 接下来进行归并排序,同时统计当逆序对元素分别在左右子数组中的情况
		int i = begin,j = mid+1,pos = begin,count = 0;
		while(i<=mid && j<=end){
			if(array[i] > array[j]){
				count += end-j+1;
				count %= 1000000007;// 避免逆序对个数超过int上限,每次计算时进行取余
				merge[pos++] = array[i++];
			}else{
				merge[pos++] = array[j++];
			}
		}
		// 处理剩余元素
		while(i<= mid) merge[pos++] = array[i++];
		while(j<= end) merge[pos++] = array[j++];
		// 返回三种情况下的逆序对个数
		// 避免逆序对个数超过int上限,每次计算时进行取余
		return ((left + right)%1000000007 + count)%1000000007;
	}
}