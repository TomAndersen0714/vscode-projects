#ifndef _TEST_H_
#define _TEST_H_

// 测试类型转换符 static_cast<type_name>(expression)
void test_static_cast();


// 测试类型转换符 reinterpret_cast<type_name>(expression)
void test_reinterpret_cast();


// 测试 time.h: clock() 计时功能,单位为毫秒
void test_clock();
#endif