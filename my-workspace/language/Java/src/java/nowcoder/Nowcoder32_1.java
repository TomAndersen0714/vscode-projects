package java.nowcoder;
// 输入一个正整数数组，把数组里所有数字拼接起来排成一个数，打印能拼接出的所有数字中最小的一个。
// 例如输入数组{3，32，321}，则打印出这三个数字能排成的最小数字为321323。
public class Nowcoder32_1{
    // 方法2:排序
    // 首先依旧是确定排序顺序,要想实现数组按照某种顺序拼接打印出来的结果
    // 对应的数值是最小的,就需要对原数组进行排序,关键在于排序的规则.
    // 排序规则:若num1+""+num2的值大于num2+""+num1,则不论这两者之间插入
    // 什么样的值,这一性质都不会改变,因此为了整体最小,num1应该排在num2之后
    // 这样便确定了数组中元素的某种顺序.
    // 方法2与方法1不同,不使用Arrays.sort方法,直接手写快排实现排序,最后输出
    public String PrintMinNumber(int [] array) {
        // 排除特殊情况
        if(array == null || array.length == 0) return "";
        // 对原数组按照既定规则进行快排
        quickSort(array,0,array.length-1);
        // 拼接数字并返回
        StringBuilder strBuilder = new StringBuilder();
        for(int num:array){
            strBuilder.append(num);
        }
        // 返回
        return strBuilder.toString();
    }
    
    // 确定排序规则
    private int compare(int num1,int num2){
        return (num1+""+num2).compareTo(num2+""+num1);
    }
    
    // 快排
    private void quickSort(int[] array,int begin,int end){
        // 排除特殊情况
        if(begin >= end) return;
        int left = begin,right = end;
        int sensor = array[left];
        while(left < right){
            while(left < right && compare(array[right],sensor)>=0) right--;
            array[left] = array[right];
            while(left < right && compare(array[left],sensor)<=0) left++;
            array[right] = array[left];
        }
        array[left] = sensor;
        quickSort(array,begin,left-1);
        quickSort(array,left+1,end);
    }
}