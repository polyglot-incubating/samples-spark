# SPARK 2

## Spark 정의
개발자 관점에서 Spark 를 정의 내린다면 RDD + Interface 이다.

### RDD (Resilient Distributed Datasets)
스파크 내에 여러 분산 노드에 걸쳐서 저장 되는 변경이 불가능한 데이타(객체)의 집합으로 
각각의 RDD 는 여러개의 파티션으로 분리가 될 수 있다.

 **RDD 특징**
   - Immutable
   - Patitioned collections of records
   - FS -> RDD 변환 가능
   - RDD<A> -> RDD<B> 변환 가능
   
### Interface
데이터를 변환 하고 처리를 주관 한다. 
- **Transformations:** 기존의 데이타프레임(RDD)을 변경하여 새로운 데이타프레임(RDD)을 생성해내는 것으로,  
map, filter, flatMap, sample, groupByKey, reduceByKey, union, join, cogroup,  
crossProduct, mapValues, sort, partitionBy, ... 등이 있다.

- **Actions:** 데이타프레임(RDD)을 기반으로 어떤 산출된 값을 계산하여(computation) 결과를 생성해내는 것으로,  
count, reduce, collect, lookup, save, ... 등이 있다.

### Spark vs Hadoop 프로세스

**Hadoop** HadoopMR 이 힘든 이유는 HDFS 상의 여러 파일을 다루면서 처리 한다.
~~~
  1. Read File 
    -> 2. Handle Data (Step 1) 
      -> 3. Write File 
        -> 4. Read File 
          -> 5(n). Handle Data (Step n) ...
~~~

**Spark** Spark는 여러 파일을 넘처나는 RAM 으로 처리 한다.
~~~
  1. Read File 
    -> 2. Handle Data (Step 1) 
      -> 3. Handle Data (Step 2) ...
~~~

- Why RAM? 
~~~
RAM 상에서 데이터를 상태를 변경하면서 처리 하기엔.... 뭔가 복잡하고 처리가 복잡하다... 
   > RAM 이니까 데이타를 ReadOnly 로 처리해보자.
      > 뭔가 데이터 처리가 쉽게 된다. RDD (Resilient Distributed Datasets)
~~~

- Spark 는 자료가 어떻게 변해갈지 DataTransformation 에서는 실제 계산(처리) 하지 않는다.
~~~
Directed Acyclic Graph(DAG) 로 디자인 해 가는것 
-> 데이터 변환 과정의 Lineage(계보)를 설계해 가는 것.

So What to do? DAG 하면 된다.
  tran
     -> tran 
        -> tran 
           -> tran 
              -> action 
action 은 처리 결과좀 줘봐 이며, 이전까지의 모든 과정은 lazy-execution 으로 명령문만 stack 에 쌓인다.
~~~

- RAM 을 ROM 처럼 썼더니 빅데이터 처리 플랫폼의 전설이 되었다.

### Spark 구성

** Spark 코어 ** 
- 작업, 스케줄링, 메모리관리, 장애 복구, 스토리지 연동
- DataFrame(RDD)를 생성 하고, 조작 하는 API 제공 

** 클러스터 매니저 **
- 효과적으로 성능을 확장  할 수 있도록 분산 노드를 관리
- Hadoop Yarn, Apache Mesos, Stand Alone

** 워크로드 컴포넌트 **
- Spark SQL: Hive 테이블, 파케이(Parquet), JSON 등 다양한 정형 데이터 소스 지원
- Spark Streaming: 실시간 스트리밍 처리를 지원하는 API 를 제공
- MLib: 분류,회기,클러스터링, 협업필터링 등 일반적인 ML 알고리즘 지원과 ROW 레벨의 최적화 알고리즘 지원
- GraphX: RDD API를 확장, 일반적인 그래프 알고리즘의 라이브러리를 지원 

### Spark 핵심 컴포넌트
~~~

드라이버[Spark Context] - 클러스터매니저(Yarn,Mesos,Standalone) - [WorkLoader[Executor[{Task,...}]]]
~~~
- 드라이버: main 함수를 포함하는 사용자 정의 프로그램
- Spark Context: 클러스터에 대한 연결 및 RDD 를 처리 하는 API 제공
- Executor: 프로그램이 실행 되는 머신
**대화형 쉘 지원**
- Python: bin/pyspark
- Scala: bin/spark-shell

### Spark 개발 과정
~~~
1. 코드 구현 -> 2. 빌드  스크립트 작성 -> 3. spark-submit 제출
~~~


### Spark 샘플 파일

- **Spark 컨텍스트**  
Spark 프로그래밍은 컨텍스트를 열고 닫는 것부터가 시작이다.
~~~
    private static JavaSparkContext getLocalContext(final String appName) {
        // @formatter:off
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(appName)
                .set("spark.executor.memory",
                "1G");
        return new JavaSparkContext(conf);
        // @formatter:on
    }

    public static void main(String[] args) {
        JavaSparkContext ctx = getLocalContext("SparkApp");
        System.out.println("appName: " + ctx.appName());
        ctx.close();
    }
~~~


#### Spark 기본 

| **Example**        | **Description**           | 
| ------------- |:-------------| 
| ex01   | Spark 에 대한 아주 기초 적인 예제를 통해 기본 개념을 익힌다. | 
| ex01.Ex001SparkContext      | SparkContext 는 스파크 기능을 사용 하기 위한 메인 진입점 이다. 이를 통해 로컬 또는 분산 클러스터 환경 기반에서 RDD 를 다룰 수 있다. | 
| ex01.Ex002RddBasic          | 컬렉션과 파일을 JavaRDD 로 변환 처리 샘플 |
| ex01.Ex003RddBasicFilter    | RDD 의 데이터 중 문자 패턴 매칭을 통한 필터링 샘플 |
| ex01.Ex004RddUnion    | 두개의 RDD 를 union 처리 샘플 |
| ex01.Ex005UserFunction    | 사용자 정의 Pure 함수를 통한 필터링 |
| ex01.Ex006MapAndFilter    | map 을 통한 데이터 변환과 필터링 하는 간단한 Pipeline 샘플 |
| ex01.Ex007FlatMap    | flatMap 을 통한 데이터 데이터 변환 샘플 |
| ex01.Ex008RddTransformation    | RDD 에 대한 종합 적인 변환 샘플 (filter, map, distinct, internal split, union, substract, intersection, cartesian)  |
| ex02   | Spark 에 대한 조금은 응용을 필요하고, 데이타를 집계 하고 현황을 구성하는 예제 |
| ex02.Ex201KeyValueFilter    | 대량의 데이터를 처리 하려면 key 를 통해 데이터를 관계 하거나, 집계할 필요가 있다. 이를 위한 샘플 |
| ex02.Ex202WordCountMapReduce    | KeyValue 와 MapReduce 를 참고할 만한 응용 기법 |
| ex02.Ex203WordCountOptimized    | Pure 함수를 통한 Ex202WordCountMapReduce 프로그램의 최적화 |
| ex02.Ex204AvgCount    | 전체 합산 및 평균을 구하는 aggregate 샘플  |
  
#### Spark 파일 처리 
Spark 는 파일과 스트림 데이터 처리에 대한 강점이 있다. 특히 large 파일에 대한 처리가 많으므로 파일에 대한 전송 지연과 디스크 가용량 최적화가 필요 하기에 압축 파일을 잘 다루어야 한다.
| **Example**        | **Description**           | 
| ------------- |:-------------| 
| file.FileWriteExample  | text, json, csv, parquet 파일 쓰기 샘플  |
| file.FileReadExample  | text, json, csv, parquet 파일 읽기 샘플  |
| file.JsonExample    | ut1001_loadFormedJson(): Spark 에서 다루는 WellFormed 스키마 데이터에 대한 데이터프레임으로 변환 처리  |
|   | ut1002_wellFormedJson(): Spark 에서 다루는 WellFormed 스키마의 파일에 대한 데이터프레임으로 변환 처리 |
|   | ut1003_malFormedJson(): 데이터가 하나의 ROW 로 된 표준 JSON 규격에 대한 데이터프레임으로 변환 처리  |
|   | ut1004_malFormedJsonCustomParser(): 데이터가 하나의 ROW 로 된 표준 JSON 규격에 대해 사용자 정의 PoJo 로 바인딩 하고 RDD 로 변환 처리  |
|   | ut1005_malFormedJsonCustomDatasetParser(): SparkSession 을 통한 표준 JSON 규격의 데이터를 데이터프레임으로 변환 처리  |
| file.CsvExample    | ut1001_samplesCSV(): CSV 파일 로딩 샘플  |
|   | ut1002_samplesCSVWithHeader(): 구분자와 헤더에 대한 옵션 추가  |
|   | ut1003_customDilimeterException(): 사용자 정의 구문자에 대한 처리  |
|   | ut1004_customBeanWithDilimeter(): 데이터 프레임의 스키마를 PoJo 객체로 변환  |
|   | ut1005_customDilimeterWithSchema(): 데이터 프레임의 스키마를 동적 스키마로 변환  |
|   | ut1006_customDilimeterWithSchemaJavaRDD(): 커스텀 구분자를 통한 RDD 데이터를 동적 구조의 데이터 프레임으로 변환  |
|   | ut1007_skipRowsInDataFrame()(): 데이터프레임에 인덱스 칼럼을 추가 하고 특정 row 를 필터링 하는 샘플  |
| zip.FileCompression  | Spark 에서 파일을 다루는 샘플  |
|   | test_writeJsonCompressFile(): 데이터프레임을 GZIP 파일로 저장 하는 샘플 |
|   | test_readFromUnCompressFile(): GZIP 파일을 읽어 데이터프레임으로 변환 하는 샘플 |
  
#### Spark JDBC 처리 
Spark 에서 NativeSQL 을 다루거나, 데이터프레임 과 RDBMS 간의 데이터 처리에 대한 예제   
| **Example**        | **Description**           | 
| ------------- |:-------------| 
| sql.ImportTranslatedData  | Spark 환경에서 Native SQL 을 처리 하거나, 데이터프렘임을 다루는 샘플  |
|   | ut1001_createTable(): Spark 환경에서 Native SQL 을 통한 테이블 생성 |
|   | ut1002_checkDataFrame(): JDBC 로 데이터를임포트 하기전 데이터 프레임확인 |
|   | ut1003_importAnonymousTable(): Spark 의 데이터프레임을 RDBMS 임시 테이블로 임포트 |
|   | ut1004_importUserTable(): Spark 의 데이터프레임을 RDBMS 사용자 정의 테이블로 임포트 |
|   | ut1005_loadFromUserTable(): RDBMS 의 테이블 데이터를 Spark 의 데이터프레임으로 변환 |

#### Spark ML(Machine Learning)
Spark MLlib 를 통해 다양한 머신 러닝 알고리즘을 응용 할 수 있다.
- ml.binary.BinarizerExample  
특정 조건(TCA)에 따른 true / false 를 판단 하여 target 을 계산 한다.

- ml.binary.BinaryClassificationMetricsExample
LogisticRegression 중 L-BFGS 알고리즘을 사용 하여 학습하는 샘플





   






### 참고
Spark 에서 성능 이슈가 있다고 하니, 팀장이 Python 으로 구현되었냐고 물어왔다. Spark 에선 Scala 가 답이다. (2015 년 기준)
http://emptypipes.org/2015/01/17/python-vs-scala-vs-spark/



