public class solution {
    public static void main(String[] args) {

        // String str_1 = new StringBuilder("xy").append("z").toString();
        // System.out.println(str_1.intern() == str_1);// true
        

        // String str_2 = new String("xyz");
        // System.out.println(str_2.intern() == str_2);// false
        // // 说明new创建的字符串不在字符串常量池中

        // String str1 = "a";
        // String str2 = "b";
        // String str3 = "ab";
        // String str4 = str1 + str2;
        // String str5 = new String("ab");
        // String str6 = "a"+"b";

        // System.out.println(str5.equals(str3));// true
        // // 说明str5和str3内容相同
        // System.out.println(str5 == str3);// false
        // // 说明str5不在常量池中
        // System.out.println(str3.intern() == str3);// true
        // // 说明str3在字符串常量池中
        // System.out.println(str4.intern() == str4);// false
        // // 说明str4并没有在字符串常量池中
        // System.out.println(str5.intern() == str6);// false
        // // 说明str6在字符串常量池中
        int a=1;
        int b=-a;
        System.out.println(b);
    }
}