package nowcoder;
// 在数组中的两个数字，如果前面一个数字大于后面的数字，则这两个数字组成一个逆序对。
// 输入一个数组,求出这个数组中的逆序对的总数P。并将P对1000000007取模的结果输出。 即输出P%1000000007
// 数组中没有重复数字
public class Nowcoder35_2{
	// 方法3:分治(递归)+归并排序
	// 思路依旧和方法2相同,只不过不采用拷贝原始数组的方式,而是每次递归时
	// 创建辅助数组,用于进行归并,然后再赋值给原始数组.
	// 时间复杂度为:O(nlogn),空间复杂度:O(n)
	public int InversePairs(int [] array) {
		// 排除特殊情况
		if(array == null || array.length <= 1) return 0;
		// 进行递归求解,并返回逆序对个数
		return divideAndMerge(array,0,array.length-1);
	}
	
	// 分治(递归)+归并排序
	private int divideAndMerge(int[] array,int begin, int end){
		// 排除特殊情况,同时设置递归出口
		if(end - begin < 1) return 0;
		// 将数组一分为二,分别获取逆序对元素同时在左子数组或者右子数组情况下的逆序对个数
		int mid = begin + (end - begin)/2;
		int left = divideAndMerge(array,begin,mid);
		int right = divideAndMerge(array,mid+1,end);
		// 使用归并排序统计ab分别在左右子数组中的情况
		// 每次归并结束时,array数组中begin~end的部分是升序排列的
		// 并且由于先对左右子数组进行了归并,因此左右子数组也是有序的
		int[] merge = new int[end - begin + 1];// 创建辅助数组用于归并,归并顺序为从大到小
		int i=begin, j=mid+1,pos=0, count=0;
		while(i<=mid && j<=end){
			if(array[i] > array[j]){
				count += end-j+1;
				count %= 1000000007;
				merge[pos++] = array[i++];
			}else{
				merge[pos++] = array[j++];
			}
		}
		// 处理剩余未归并元素
		while(i<=mid) merge[pos++] = array[i++];
		while(j<=end) merge[pos++] = array[j++];
		// 将归并数组中的值放回到原始数组中
		System.arraycopy(merge,0,array,begin,merge.length);
		// for(int l=0;l<pos;l++){
			// array[begin+l]=merge[l];
		// }
		// 返回逆序对个数
		return ((left + right)%1000000007 + count)%1000000007;
	}
}