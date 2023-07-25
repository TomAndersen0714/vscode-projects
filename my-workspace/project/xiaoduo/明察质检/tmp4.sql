Airflow DAG告警
Env: tb
DAG: voc_ods_etl_jd, Task: dwd_voc_chat_log_detail_etl
Owner: chengcheng, Email: ['chengcheng@xiaoduotech.com']
State: up_for_retry
Try: 5 out of 7
Exception: Code: 241.
DB::Exception: Memory limit (for query) exceeded: would use 12.62 GiB (attempt to allocate chunk of 4295527472 bytes), maximum: 9.31 GiB: while executing 'FUNCTION arrayElement(sorted_msg_acts :: 2, minus(y, 1) :: 3) -> arrayElement(sorted_msg_acts, minus(y, 1)) String : 1': while executing 'FUNCTION arrayMap(__lambda :: 6, sorted_msg_acts :: 7, arrayEnumerate(sorted_msg_acts) :: 13) -> arrayMap(lambda(tuple(x, y), if(and(equals(x, 'send_msg'), equals(arrayElement(sorted_msg_acts, minus(y, 1)), 'recv_msg')), 1, 0)), sorted_msg_acts, arrayEnumerate(sorted_msg_acts)) Array(UInt8) : 12'. Stack trace:

0. DB::Exception::Exception(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, int, bool) @ 0x8fe8d5a in /usr/bin/clickhouse
1. DB::Exception::Exception<char const*, char const*, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, long&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > >(int, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, char const*&&, char const*&&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >&&, long&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >&&) @ 0x8ffe749 in /usr/bin/clickhouse
2. MemoryTracker::allocImpl(long, bool) @ 0x8ffe0fc in /usr/bin/clickhouse
3. MemoryTracker::allocImpl(long, bool) @ 0x8ffde54 in /usr/bin/clickhouse
4. COW<DB::IColumn>::immutable_ptr<DB::IColumn> DB::FunctionArrayElement::executeString<long>(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, DB::PODArray<long, 4096ul, Allocator<false, false>, 15ul, 16ul> const&, DB::ArrayImpl::NullMapBuilder&) @ 0xed2a6e4 in /usr/bin/clickhouse
5. COW<DB::IColumn>::immutable_ptr<DB::IColumn> DB::FunctionArrayElement::executeArgument<long>(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, DB::ArrayImpl::NullMapBuilder&, unsigned long) const @ 0xed19283 in /usr/bin/clickhouse
6. DB::FunctionArrayElement::perform(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, DB::ArrayImpl::NullMapBuilder&, unsigned long) const @ 0xed13731 in /usr/bin/clickhouse
7. DB::FunctionArrayElement::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long) const @ 0xed11c5d in /usr/bin/clickhouse
8. DB::FunctionToExecutableFunctionAdaptor::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long) const @ 0xb18690e in /usr/bin/clickhouse
9. DB::IExecutableFunction::executeWithoutLowCardinalityColumns(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long, bool) const @ 0xfb3689e in /usr/bin/clickhouse
10. DB::IExecutableFunction::execute(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long, bool) const @ 0xfb36eb2 in /usr/bin/clickhouse
11. DB::ExpressionActions::execute(DB::Block&, unsigned long&, bool) const @ 0x101b3d55 in /usr/bin/clickhouse
12. DB::ExpressionActions::execute(DB::Block&, bool) const @ 0x101b4e02 in /usr/bin/clickhouse
13. DB::ExecutableFunctionExpression::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long) const @ 0x102bfac5 in /usr/bin/clickhouse
14. DB::IExecutableFunction::executeWithoutLowCardinalityColumns(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long, bool) const @ 0xfb3689e in /usr/bin/clickhouse
15. DB::IExecutableFunction::execute(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long, bool) const @ 0xfb36eb2 in /usr/bin/clickhouse
16. DB::IFunctionBase::execute(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long, bool) const @ 0xb186407 in /usr/bin/clickhouse
17. DB::ColumnFunction::reduce() const @ 0x108e01a2 in /usr/bin/clickhouse
18. DB::FunctionArrayMapped<DB::ArrayMapImpl, DB::NameArrayMap>::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long) const @ 0xecf7344 in /usr/bin/clickhouse
19. DB::FunctionToExecutableFunctionAdaptor::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long) const @ 0xb18690e in /usr/bin/clickhouse
20. DB::IExecutableFunction::executeWithoutLowCardinalityColumns(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long, bool) const @ 0xfb3689e in /usr/bin/clickhouse
21. DB::IExecutableFunction::execute(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> > const&, std::__1::shared_ptr<DB::IDataType const> const&, unsigned long, bool) const @ 0xfb36eb2 in /usr/bin/clickhouse
22. DB::ExpressionActions::execute(DB::Block&, unsigned long&, bool) const @ 0x101b3d55 in /usr/bin/clickhouse
23. DB::ExpressionTransform::transform(DB::Chunk&) @ 0x11283c9c in /usr/bin/clickhouse
24. DB::ISimpleTransform::transform(DB::Chunk&, DB::Chunk&) @ 0x11284030 in /usr/bin/clickhouse
25. DB::ISimpleTransform::work() @ 0x11286f07 in /usr/bin/clickhouse
26. ? @ 0x1113895d in /usr/bin/clickhouse
27. DB::PipelineExecutor::executeStepImpl(unsigned long, unsigned long, std::__1::atomic<bool>*) @ 0x111354f1 in /usr/bin/clickhouse
28. ? @ 0x11139f96 in /usr/bin/clickhouse
29. ThreadPoolImpl<std::__1::thread>::worker(std::__1::__list_iterator<std::__1::thread, void*>) @ 0x90299df in /usr/bin/clickhouse
30. ? @ 0x902d2c3 in /usr/bin/clickhouse
31. start_thread @ 0x9609 in /usr/lib/x86_64-linux-gnu/libpthread-2.31.so

Log: log_url
