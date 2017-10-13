package org.chiwooplatform.samples.ml.binary;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.junit.Test;

import scala.Tuple2;

/**
 * SVM(Support Vector Machine)은 자료 분석을 위한 지도 학습 모델 중 패턴 인식 기법을 기반으로 하며 주로 회귀 분석을 위해 사용한다.
 * 
 * <pre>
 * SVM 데이터 포멧  
 * 0 128:51 129:159 130:253 ...
 * 1 159:124 160:253 161:255 ...
 * " " 공백 split 으로 Array 를 구성 하고, 
 *   - 첫번째 요소가 label 이며(0 또는 1을 결정) 
 *   - 두번째 요소 부터  ":" 캐릭터로 split 하여 Array 구성 한다.
 *     : 첫번째 요소가 인덱스 이며
 *     : 두번째 요소는 인덱스에 대응하는 값
 *     이 된다.
 *   row 단위로 Vector 를 구성 한다면, Vector[label, indices, values] 가 된다.
 * Vector[0, {128, 129, 130}, {51, 159, 253}]
 * Vector[1, {159, 160, 161}, {124, 253, 255}]
 * 
 * 참고로, Spark MLLIB는 내부적으로, SVM 변환시 인덱스에 대해, 0부터 색인 되도록, 인덱스 값에 -1 씩 차감을 하고 있다. 
 * 그러므로 실제 Spark SVM Vector 구성은 아래와 같다.
 * Vector[elementsSize, {127, 128, 129}, {51, 159, 253}]
 * Vector[elementsSize, {158, 159, 160}, {124, 253, 255}]
 * 
 * elementsSize 는 전체 앨리먼트의 갯수를 의미한다. 중요한 것은 index 의 값은 elementsSize 범위내에 있어야 한다.
 * </pre>
 * 
 * <pre>
 * 이 예제는, LogisticRegression 중 L-BFGS 알고리즘을 사용 하여 학습 시켰다. (60% 의 데이터를 랜덤으로 구성)
 * 로지스틱 회기 함수의 특징은, x의 값이 증가하던 감소하던 이와는 무관하게 f(x)의 결과는 0과 1 중 하나의 값을 반환 한다. 
 *  (ex — 학생이 문제를 맞을 것인지 틀릴 것인지, 내일 비가 올지 안올지, 과목의 학점이 A인지 B인지 C인지)
 * L-BFGS 알고리즘 장점은,  leanring rate 를 고를 필요가 없고, 대부분 gradient decsent 알고리즘 보다 빠르다.
 * 
 * 참고로, 로지스틱 회기 알고리즘은 크게 (Conjugate gradient, BFGS, L-BFGS) 가 있다.
 * </pre>
 * 
 * <pre>
 * ROC(Receiver Operating Characteristic) Curve 는,  참(true)일 확율 y, 거짓(false)일 확율 x 값을 
 * 그래프로 표현한 것으로, ROC 결과 값이 1 에 가까을 수록 완벽 하며, 0.5 는  쓸모가 없다.
 * 
 * 참고 자료의 그래프 확인 - https://synapse.koreamed.org/pdf/10.4082/kjfm.2009.30.11.841
 * </pre>
 */
public class BinaryClassificationWithSVM {

    @Test
    public void ut1001_testVector() throws Exception {
        final Vector v = Vectors.dense(1.0, 0.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0);
        System.out.println(v);

        final int[] indices = { 127, 128, 129 };
        final double[] values = { 51.0, 159.0, 253.0 };
        final Vector indexsAndValuesVector = Vectors.sparse(indices.length, indices, values);
        System.out.println(indexsAndValuesVector.toJson());
    }

    @Test
    public void ut1002_traningBasicLBFGSModel() throws Exception {
        final String path = "target/model/BasicLBFGSModel";
        JavaSparkContext spark = SparkContextHolder.getLocalContext("traningBasicLBFGSModel");
        List<LabeledPoint> list = new ArrayList<>();
        LabeledPoint zero = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0));
        LabeledPoint one = new LabeledPoint(1.0, Vectors.dense(8.0, 7.0, 6.0, 4.0, 5.0, 6.0, 1.0, 2.0, 3.0));
        list.add(zero);
        list.add(one);
        JavaRDD<LabeledPoint> data = spark.parallelize(list);
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(data.rdd());
        model.save(spark.sc(), path);
        spark.close();
    }

    @Test
    public void ut1003_predicateBasicLBFGSModel() throws Exception {
        final String path = "target/model/BasicLBFGSModel";
        JavaSparkContext spark = SparkContextHolder.getLocalContext("predicationBasicLBFGSModel");
        final LogisticRegressionModel model = LogisticRegressionModel.load(spark.sc(), path);
        double result = model.predict(Vectors.dense(0.0, 3.0, 11.0, 9.0, 7.0, 6.0, 1.0, 2.0, 4.0));
        System.out.println("result: " + result);
        spark.close();
    }

    @Test
    public void ut1004_testSvmVectors() throws Exception {
        final int[] indicesForTrue = { 1, 2, 3 };
        final double[] valuesForTrue = { 51.0, 159.0, 253.0 };

        final int[] indicesForFalse = { 11, 12, 13 };
        final double[] valuesForFalse = { 1.0, 9.0, 1053.0 };
        LabeledPoint trueLabel = new LabeledPoint(1,
                Vectors.sparse(indicesForTrue.length, indicesForTrue, valuesForTrue));
        LabeledPoint falseLabel = new LabeledPoint(0,
                Vectors.sparse(indicesForFalse.length, indicesForFalse, valuesForFalse));
        System.out.println(trueLabel.toString());
        System.out.println(falseLabel.toString());
    }

    @Test
    public void ut1005_traningWithSVMVectors() throws Exception {
        final String path = "target/model/SVMLBFGSModel";
        JavaSparkContext spark = SparkContextHolder.getLocalContext("traningSVMLBFGSModel");
        final int totalElements = 36;
        final int E = totalElements;
        List<LabeledPoint> list = new ArrayList<>();
        list.add(new LabeledPoint(0.0, Vectors.sparse(E, new int[] { 0, 1, 2 }, new double[] { 1.0, 19.0, 31.0 })));
        list.add(new LabeledPoint(0.0, Vectors.sparse(E, new int[] { 3, 4, 5 }, new double[] { 6.0, 9.0, 63.0 })));
        list.add(new LabeledPoint(0.0, Vectors.sparse(E, new int[] { 4, 5, 6 }, new double[] { 6.0, 9.0, 63.0 })));
        list.add(new LabeledPoint(1.0, Vectors.sparse(E, new int[] { 7, 8, 9 }, new double[] { 51.0, 19.0, 23.0 })));
        list.add(new LabeledPoint(1.0, Vectors.sparse(E, new int[] { 10, 11, 12 }, new double[] { 33.0, 77.0, 31.0 })));
        list.add(new LabeledPoint(0.0, Vectors.sparse(E, new int[] { 13, 14, 15 }, new double[] { 12.0, 21.0, 25.0 })));
        list.add(new LabeledPoint(1.0, Vectors.sparse(E, new int[] { 16, 17, 18 }, new double[] { 18.0, 17.0, 26.0 })));
        list.add(new LabeledPoint(1.0, Vectors.sparse(E, new int[] { 19, 20, 21 }, new double[] { 5.0, 59.0, 25.0 })));
        list.add(new LabeledPoint(1.0, Vectors.sparse(E, new int[] { 23, 24, 25 }, new double[] { 51, 19, 2.0 })));
        list.add(new LabeledPoint(0.0, Vectors.sparse(E, new int[] { 26, 27, 28 }, new double[] { 19.0, 59.0, 39.0 })));
        list.add(new LabeledPoint(0.0, Vectors.sparse(E, new int[] { 30, 31, 32 }, new double[] { 14.0, 94.0, 43.0 })));
        list.add(new LabeledPoint(0.0, Vectors.sparse(E, new int[] { 33, 34, 35 }, new double[] { 37.0, 7.0, 8.0 })));
        JavaRDD<LabeledPoint> data = spark.parallelize(list);
        // SparkUtils.log(data.take(10));
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(data.rdd());
        model.save(spark.sc(), path);
        spark.close();
    }

    @Test
    public void ut1006_predicateSVMLBFGSModel() throws Exception {
        final String path = "target/model/SVMLBFGSModel";
        JavaSparkContext spark = SparkContextHolder.getLocalContext("traningSVMLBFGSModel");
        final LogisticRegressionModel model = LogisticRegressionModel.load(spark.sc(), path);
        System.out.println("model: " + model);
        final int totalElements = 36, E = totalElements;
        final Vector input = Vectors.sparse(E, new int[] { 11, 21, 26 }, new double[] { 3.0, 17.0, 29.0 });
        double result = model.predict(input);
        System.out.println("CASE 1 result: " + result);

        final Vector input2 = Vectors.sparse(E, new int[] { 12, 20, 21 }, new double[] { 5.0, 59.0, 25.0 });
        double result2 = model.predict(input2);
        System.out.println("CASE 2 result: " + result2);
        spark.close();
    }

    /**
     * 로지스틱 회기 모델 중 L-BFGS 알고리즘을 통한 ML 학습 및 평가
     * @throws Exception
     */
    @Test
    public void ut1007_binaryClassificationWithSVM() throws Exception {
        SparkContext spark = SparkContextHolder.getLocalContext("BinarizerExample").sc();

        String path = SparkUtils.resourcePath("data/mllib/sample_binary_classification_data.txt");
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(spark, path).toJavaRDD();

        // RDD 중 [60% training data, 40% testing data]로 구분 하기 위한 splits 정의
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] { 0.6, 0.4 }, 11L);
        JavaRDD<LabeledPoint> training = splits[0].cache(); // 첫번째는 training 셋
        JavaRDD<LabeledPoint> test = splits[1]; // 두번째는 test 셋
        // test.take(1).get(0).features()
        // L-BFGS 알고리즘을 통한 학습 진행
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training.rdd());

        // Clear the prediction threshold so the model will return probabilities
        model.clearThreshold();

        // Compute raw scores on the test set.
        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p -> {
            final double label = p.label();
            final Vector v = p.features();
            System.out.printf("label: %s, features(Vector): '%s'\n", label, v);
            return new Tuple2<>(model.predict(v), label);
        });

        // Get evaluation metrics.
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd());

        // Precision by threshold
        JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();

        SparkUtils.log(precision.take(100));
        System.out.println("Precision by threshold: " + precision.collect());

        // Recall by threshold
        JavaRDD<?> recall = metrics.recallByThreshold().toJavaRDD();
        System.out.println("Recall by threshold: " + recall.collect());

        // F Score by threshold
        JavaRDD<?> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
        System.out.println("F1 Score by threshold: " + f1Score.collect());

        JavaRDD<?> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
        System.out.println("F2 Score by threshold: " + f2Score.collect());

        // Precision-recall curve
        JavaRDD<?> prc = metrics.pr().toJavaRDD();
        System.out.println("Precision-recall curve: " + prc.collect());

        // Thresholds
        JavaRDD<Double> thresholds = precision.map(t -> Double.parseDouble(t._1().toString()));
        System.out.println("Thresholds: " + thresholds.collect());

        // ROC Curve
        JavaRDD<?> roc = metrics.roc().toJavaRDD();
        System.out.println("ROC curve: " + roc.collect());

        // AUPRC
        System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());

        // AUROC
        System.out.println("Area under ROC = " + metrics.areaUnderROC());

        // Save and load model
        model.save(spark, "target/model/LogisticRegressionModel");
        LogisticRegressionModel.load(spark, "target/model/LogisticRegressionModel");
        spark.stop();
    }

}
