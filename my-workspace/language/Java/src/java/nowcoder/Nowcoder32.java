package java.nowcoder;
import java.util.Arrays;
// 输入一个正整数数组，把数组里所有数字拼接起来排成一个数，打印能拼接出的所有数字中最小的一个。
// 例如输入数组{3，32，321}，则打印出这三个数字能排成的最小数字为321323。
public class Nowcoder32{
    // 方法1:排序
    // 本题主要在于如何确定数组中数字的顺序,保证按照某种顺序输出数组中的元素时
    // 数组中的数组能够顺次组合成一个最小数.排序规则为:若num1+""+num2组成的数
    // 大于num2+""+num1组成的数,则表明num1应该处于num2之后才能保证整体较小.
    // 注意:不能转换成Int比较大小,因为一旦数据过大就会发生数据溢出
    public String PrintMinNumber(int [] numbers) {
        // 排除特殊情况
        if(numbers == null || numbers.length == 0) return "";
        // 否则先将原始数组转换成封装类,因为基本数据类型数组不能使用Arrays工具类进行自定义排序
        // 使用Arrays.sort方法对基本数据类型的数组只能按照字典升序排序
        Integer[] array = new Integer[numbers.length];
        for(int i=0; i<array.length; i++){
            array[i] = new Integer(numbers[i]);
        }
        // 使用Arrays.sort方法对Integer数组进行排序
        Arrays.sort(array,(o1,o2)->{
            return (o1+""+o2).compareTo(o2+""+o1);
        });
        // 遍历数组拼接数字后输出
        StringBuilder strBuilder = new StringBuilder();
        for(Integer num:array){
            strBuilder.append(num);
        }
        return strBuilder.toString();
    }
}