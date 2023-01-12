# Java中i++和++i原理解析

## 直接上代码：

```java
public class Test {
	public static void main(String[] args) {
		int i = 0,j = 0;
		i=i++;
		j=++j;
		System.out.println(i);
		System.out.println(j);
	}
}
```

## 反编译class文件：

```java
Compiled from "Test.java"
public class Test {
  public Test();
    Code:
       0: aload_0
       1: invokespecial #1                  // Method java/lang/Object."<init>":()V
       4: return

  public static void main(java.lang.String[]);
    Code:
       0: iconst_0
       1: istore_1
       2: iconst_0
       3: istore_2
       4: iload_1
       5: iinc          1, 1
       8: istore_1
       9: iinc          2, 1
      12: iload_2
      13: istore_2
      14: getstatic     #2                  // Field java/lang/System.out:Ljava/io/PrintStream;
      17: iload_1
      18: invokevirtual #3                  // Method java/io/PrintStream.println:(I)V
      21: getstatic     #2                  // Field java/lang/System.out:Ljava/io/PrintStream;
      24: iload_2
      25: invokevirtual #3                  // Method java/io/PrintStream.println:(I)V
      28: return
}

```

## 翻译：

