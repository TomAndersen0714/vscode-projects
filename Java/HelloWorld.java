import java.util.Arrays;

public class HelloWorld {
    public static void main(String[] args) {
        /*
         * int x = 123; Integer y = new Integer(123); int z = y.intern();
         */
        String s1 = new String("xyz");
        // intern方法的作用是在字符串常量池中寻找满足equal方法的字符串
        // 即有相同内容的String对象的引用,如果找到了则返回其引用
        // 若未找到则将其添加到字符串常量池中,然后返回其引用
        String s2 = s1.intern();

        System.out.println(s1 == s2);// false
        // 因为s1是指向堆中String对象的引用
        // s2是指向字符串常量区中String对象的引用
        // 故两者不相同
        String s3 = "xyz";
        // 由于是直接赋值字符串字面量，所以s3是指向常量区String对象"xyz"的引用
        // 由于字符串常量区中每个String对象都是unique的，所以所有指向"xyz"字符串
        // 的引用都相同
        System.out.println(s2 == s3);// true
        // 因为s2是指向字符串常量区中String对象"xyz"的引用
        // s3也是指向字符串常量区String对象"xyz"的引用，故两者相等
        // try {
        //     throw new Exception("This is a test Exception!");
        // } catch (Exception e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // }
        // Nowcoder17_1 test = new Nowcoder17_1();
        // TreeMap<Character,Integer> treeMap = new TreeMap<>();
        char[] array = new char[]{'a','b','c'};
        System.out.println(new String(array));
        Arrays.sort(array);
        // 说明创建数组时设置的初始值,不仅仅可以使用字面量,同样可以使用变量
        int a = 1,b =2;
        int[] result = new int[]{a,b};
        System.out.println(Arrays.toString(result));
    }
}