#include <stdio.h>

#include "test.h"

int main(int argc,char* argv[]){

    // printf("Hello World!\n");

    // 测试类型转换符 static_cast<type_name>(expression)
    // test_static_cast();

    // 测试类型转换符 reinterpret_cast<type_name>(expression)
    // test_reinterpret_cast();
    
    // 测试 time.h: clock() 计时功能,单位为毫秒
    test_clock();

    return 0;
}