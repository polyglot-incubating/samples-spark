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
  1. Read File -> 2. Handle Data (Step1) -> 3. Write File 
    -> 4. Read File -> 5. Handle Data (Step2) ...
~~~

**Spark** Spark는 여러 파일을 넘처나는 RAM 으로 처리 한다.
~~~
  1. Read File -> 2. Handle Data (Step1) -> 4. Handle Data (Step2) ...
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



### 참고
Spark 에서 성능 이슈가 있다고 하니, 팀장이 Python 으로 구현되었냐고 물어왔다. Spark 에선 Scala 가 답이다. (2015 년 기준)
http://emptypipes.org/2015/01/17/python-vs-scala-vs-spark/



83 page 까진 봤다.