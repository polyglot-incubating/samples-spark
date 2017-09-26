Spark 에서 데이터를 Key / Value 쌍으로 관리 한다면, 다양하게 응용이 가능 하다.
Data 변환, Data 그룹핑 및 병합, Data 저장 및 추출 등의 작업에 대해 특정 Key 를 통해 이루어 지도록 할 수 있다.
뿐만 아니라, 대량의 데이터에 대한 분산 처리를 최적화 하여 처리 속도도 향상 시킬 수 있다.
Key/Value 데이터는 Aggregation 에서 빈번하게 활용 된다.


[Scala]
val pairs = lines.map(x => (x.split(" ")(0), x))


Key / Value Pair를 사용 하면, 2번째 앨리먼트에 대한 필터가 아래와같이 단순하게 처리 된다.
pairs.filter{case (key, value) => value.length < 20}


[Java]
PairFunction<String, String, String> keyData =  new PairFunction<String, String, String>() {
  public Tuple2<String, String> call(String x) {
      return new Tuple2(x.split(" ")[0], x);
  }
};

JavaPairRDD<String, String> pairs = lines.mapToPair(keyData); 

Function<Tuple2<String, String>, Boolean> longWordFilter =  new Function<Tuple2<String, String>, Boolean>() {
  public Boolean call(Tuple2<String, String> keyValue) {
    return (keyValue._2().length() < 20);
  }
}

Java 에선 Scala 보단 복잡하게 처리 된다.
JavaPairRDD<String, String> result = pairs.filter(longWordFilter);


