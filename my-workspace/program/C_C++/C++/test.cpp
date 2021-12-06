#include <stdio.h>
#include <time.h>

#include "test.h"

// 测试类型转换符 static_cast<type_name>(expression)
void test_static_cast()
{
    float f = 1.6f;
    int a = 16;
    int f2i = static_cast<int>(f);
    float i2f = static_cast<float>(a);
    printf("float:%f,\tstatic_cast<int>:%d\n", f, f2i);
    printf("int:%d,\tstatic_cast<float>:%f\n", a, i2f);
}

// 测试类型转换符 reinterpret_cast<type_name>(expression)
void test_reinterpret_cast()
{
    double d = 15.3;
    long long int *lli = reinterpret_cast<long long int *>(&d); // 此运算符支持相同字节长度值的指针相互转换
    // char *c = reinterpret_cast<char*>(d); // error: 此运算符不支持将长值的指针转换为短值的指针
    unsigned char *c = (unsigned char *)&d; // 通过C形式的强制转换,却能够实现将长值指针转换为短值的指针
    printf("0x%llX\n", d);
    printf("sizeof(long long int):%d,\t0x%llX\n", sizeof(long long int), *lli);
    printf("sizeof(unsigned char):%d,\t0x%X\n", sizeof(unsigned char), *c);
    // 此处不能使用char,如果使用char进行打印,C/C++则会自动将其识别为带符号整数,会先隐式将其转换为int,然后再去打印
}

// 测试 time.h: clock() 计时功能,单位为毫秒
void test_clock()
{
    clock_t start_t, end_t;
    start_t = clock();
    for (int i = 0; i < 10000000; i++)
    {
        
    }
    end_t = clock();
    printf("Interval in millis: %lld\n", (end_t - start_t) * 1000 / CLOCKS_PER_SEC);
}