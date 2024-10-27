```Mermaid
classDiagram
direction BT
class Analyzer~S, M~ {
<<Interface>>
  + loadStateAndComputeMetric(StateLoader) Option~M~
  + aggregateStateTo(StateLoader, StateLoader, StatePersister) void
  + computeMetricFrom(Option~S~) M
  + preconditions() Seq~Function1~StructType, BoxedUnit~~
  + calculate(Dataset~Row~, Option~StateLoader~, Option~StatePersister~) M
  + calculateMetric(Option~S~, Option~StateLoader~, Option~StatePersister~) M
  + toFailureMetric(Exception) M
  + computeStateFrom(Dataset~Row~) Option~S~
  + copyStateTo(StateLoader, StatePersister) void
}
class ApproxQuantile {
  + ApproxQuantile(String, double, double, Option~String~) 
  + aggregationFunctions() List~Column~
  + aggregateStateTo(StateLoader, StateLoader, StatePersister) void
  + copyStateTo(StateLoader, StatePersister) void
  + PARAM_CHECKS() Function1~StructType, BoxedUnit~
  + unapply(ApproxQuantile) Option~Tuple4~String, Object, Object, Option~String~~~
  + apply(String, double, double, Option~String~) ApproxQuantile
  + copy(String, double, double, Option~String~) ApproxQuantile
  + where() Option~String~
  + calculateMetric(Option~ApproxQuantileState~, Option~StateLoader~, Option~StatePersister~) DoubleMetric
  + column() String
  + computeMetricFrom(Option~ApproxQuantileState~) DoubleMetric
  + quantile() double
  + filterCondition() Option~String~
  + fromAggregationResult(Row, int) Option~ApproxQuantileState~
  + metricFromAggregationResult(Row, int, Option~StateLoader~, Option~StatePersister~) DoubleMetric
  + loadStateAndComputeMetric(StateLoader) Option~DoubleMetric~
  + preconditions() Seq~Function1~StructType, BoxedUnit~~
  + relativeError() double
  + productPrefix() String
  + calculate(Dataset~Row~, Option~StateLoader~, Option~StatePersister~) DoubleMetric
  + toFailureMetric(Exception) DoubleMetric
  + productIterator() Iterator~Object~
  + curried() Function1~T1, Function1~T2, Function1~T3, Function1~T4, R~~~~
  + tupled() Function1~Tuple4~T1, T2, T3, T4~, R~
  + computeStateFrom(Dataset~Row~) Option~ApproxQuantileState~
}
class GroupingAnalyzer~S, M~ {
  + GroupingAnalyzer() 
  + loadStateAndComputeMetric(StateLoader) Option~M~
  + aggregateStateTo(StateLoader, StateLoader, StatePersister) void
  + calculate(Dataset~Row~, Option~StateLoader~, Option~StatePersister~) M
  + groupingColumns() Seq~String~
  + calculateMetric(Option~S~, Option~StateLoader~, Option~StatePersister~) M
  + preconditions() Seq~Function1~StructType, BoxedUnit~~
  + copyStateTo(StateLoader, StatePersister) void
}
class KLLSketch {
  + KLLSketch(String, Option~KLLParameters~) 
  + sketchSize() int
  + productIterator() Iterator~Object~
  + shrinkingFactor_$eq(double) void
  + column() String
  + copy(String, Option~KLLParameters~) KLLSketch
  + DEFAULT_SKETCH_SIZE() int
  + calculate(Dataset~Row~, Option~StateLoader~, Option~StatePersister~) KLLMetric
  - PARAM_CHECK() Function1~StructType, BoxedUnit~
  + kllParameters() Option~KLLParameters~
  + aggregationFunctions() Seq~Column~
  + preconditions() Seq~Function1~StructType, BoxedUnit~~
  + calculateMetric(Option~KLLState~, Option~StateLoader~, Option~StatePersister~) KLLMetric
  + numberOfBuckets() int
  + DEFAULT_SHRINKING_FACTOR() double
  + productPrefix() String
  + unapply(KLLSketch) Option~Tuple2~String, Option~KLLParameters~~~
  + toFailureMetric(Exception) KLLMetric
  + numberOfBuckets_$eq(int) void
  + shrinkingFactor() double
  + sketchSize_$eq(int) void
  + fromAggregationResult(Row, int) Option~KLLState~
  + computeMetricFrom(Option~KLLState~) KLLMetric
  + MAXIMUM_ALLOWED_DETAIL_BINS() int
  + metricFromAggregationResult(Row, int, Option~StateLoader~, Option~StatePersister~) KLLMetric
  + computeStateFrom(Dataset~Row~) Option~KLLState~
  + aggregateStateTo(StateLoader, StateLoader, StatePersister) void
  + loadStateAndComputeMetric(StateLoader) Option~KLLMetric~
  + apply(String, Option~KLLParameters~) KLLSketch
  + copyStateTo(StateLoader, StatePersister) void
}
class SampleAnalyzer {
  + SampleAnalyzer(String) 
  + loadStateAndComputeMetric(StateLoader) Option~DoubleMetric~
  + apply(String) SampleAnalyzer
  + column() String
  + computeStateFrom(Dataset~Row~) Option~NumMatches~
  + copy(String) SampleAnalyzer
  + compose(Function1~A, T1~) Function1~A, R~
  + productIterator() Iterator~Object~
  + toFailureMetric(Exception) DoubleMetric
  + andThen(Function1~R, A~) Function1~T1, A~
  + preconditions() Seq~Function1~StructType, BoxedUnit~~
  + unapply(SampleAnalyzer) Option~String~
  + calculate(Dataset~Row~, Option~StateLoader~, Option~StatePersister~) DoubleMetric
  + computeMetricFrom(Option~NumMatches~) DoubleMetric
  + aggregateStateTo(StateLoader, StateLoader, StatePersister) void
  + copyStateTo(StateLoader, StatePersister) void
  + calculateMetric(Option~NumMatches~, Option~StateLoader~, Option~StatePersister~) DoubleMetric
  + productPrefix() String
}
class ScanShareableAnalyzer~S, M~ {
<<Interface>>
  + metricFromAggregationResult(Row, int, Option~StateLoader~, Option~StatePersister~) M
  + fromAggregationResult(Row, int) Option~S~
  + computeStateFrom(Dataset~Row~) Option~S~
  + aggregationFunctions() Seq~Column~
}
class StandardScanShareableAnalyzer~S~ {
  + StandardScanShareableAnalyzer(String, String, Value) 
  + metricFromAggregationResult(Row, int, Option~StateLoader~, Option~StatePersister~) DoubleMetric
  + loadStateAndComputeMetric(StateLoader) Option~DoubleMetric~
  + additionalPreconditions() Seq~Function1~StructType, BoxedUnit~~
  + calculateMetric(Option~S~, Option~StateLoader~, Option~StatePersister~) DoubleMetric
  + preconditions() Seq~Function1~StructType, BoxedUnit~~
  + computeMetricFrom(Option~S~) DoubleMetric
  + copyStateTo(StateLoader, StatePersister) void
  + toFailureMetric(Exception) DoubleMetric
  + aggregateStateTo(StateLoader, StateLoader, StatePersister) void
  + computeStateFrom(Dataset~Row~) Option~S~
  + calculate(Dataset~Row~, Option~StateLoader~, Option~StatePersister~) DoubleMetric
}

ApproxQuantile  ..>  ScanShareableAnalyzer~S, M~ 
GroupingAnalyzer~S, M~  ..>  Analyzer~S, M~ 
KLLSketch  ..>  ScanShareableAnalyzer~S, M~ 
SampleAnalyzer  ..>  Analyzer~S, M~ 
ScanShareableAnalyzer~S, M~  -->  Analyzer~S, M~ 
StandardScanShareableAnalyzer~S~  ..>  ScanShareableAnalyzer~S, M~ 

```