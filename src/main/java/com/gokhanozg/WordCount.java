package com.gokhanozg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by mephala on 3/24/17.
 */
public class WordCount {

    private static final String TRAIN_SRC = "/usr/local/programming/intellij/intellijProjects/sparkTest/src/main/resources/train.csv";
    private static final Long SPLIT_SEED = 2141251251L; // Using same seed to split in order to get same accuracy.
    private static final int THREAD = 8;

    public static void wordCountJava8(String filename) {
//        // Define a configuration to use to interact with Spark
//        SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");
//
//        // Create a Java version of the Spark Context from the configuration
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        // Load the input data, which is a text file read from the command line
//        JavaRDD<String> input = sc.textFile( filename );
//
//        // Java 8 with lambdas: split the input string into words
//        JavaRDD<String> words = input.flatMap( s -> Arrays.asList( s.split( " " ) ) );
//
//        // Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
//        JavaPairRDD<String, Integer> counts = words.mapToPair(t -> new Tuple2( t, 1 ) ).reduceByKey( (x, y) -> (int)x + (int)y );
//
//        // Save the word count back out to a text file, causing evaluation.
//        counts.saveAsTextFile( "output" );
    }

    public static void main(String[] args) {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName("MNIST processing");

        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(TRAIN_SRC);
        JavaRDD<Image> images = lines.map(s -> {
            String[] vals = s.split(",");
            Image img = new Image();
            img.label = vals[0];
            int pixelIndex = 1; //0 is label
            for (int i = 0; i < 28; i++) {
                for (int j = 0; j < 28; j++) {
                    img.pixels[i][j] = Integer.parseInt(vals[pixelIndex]);
                    pixelIndex++;
                }
            }
            return img;
        });
        double[] splitWeights = new double[]{0.7d, 0.3d};
        JavaRDD<Image>[] testTrainImages = images.randomSplit(splitWeights, SPLIT_SEED);
        JavaRDD<Image> trainImages = testTrainImages[0]; // 60% of data is train
        JavaRDD<Image> testImages = testTrainImages[1]; // 40% of data is test

        List<Image> testImagesList = testImages.collect();
        long correctus = 0;
        int run = 0;
        boolean pmStopActive = true;
        int runLimit = 100;

        for (Image testImage : testImagesList) {
            JavaRDD<CloseImage> closeImages = trainImages.map(trainImage -> {
                BigDecimal error = BigDecimal.ZERO;
                String label = trainImage.label;
                for (int i = 0; i < 28; i++) {
                    for (int j = 0; j < 28; j++) {
                        int differ = trainImage.getPixels()[i][j] - testImage.pixels[i][j];
                        error = error.add(BigDecimal.valueOf(differ * differ)); // Square of error
                    }
                }
                CloseImage ci = new CloseImage();
                ci.setError(error);
                ci.setLabel(label);
                return ci;
            });
            CloseImage closestImage = closeImages.reduce((c1, c2) -> {
                if (c1.getError().compareTo(c2.getError()) == -1) {
                    return c1;
                } else {
                    return c2;
                }
            });
            if (closestImage.getLabel().equals(testImage.getLabel())) {
                correctus++;
            }
            run++;
            if (run == runLimit && pmStopActive) {
                break;
            }
        }
        BigDecimal total = BigDecimal.valueOf(testImagesList.size());
        if (pmStopActive) {
            total = BigDecimal.valueOf(runLimit);
        }
        BigDecimal hund = BigDecimal.valueOf(100);
        BigDecimal correct = BigDecimal.valueOf(correctus);
        System.out.println("Accuracy:" + correct.multiply(hund).divide(total, 3, BigDecimal.ROUND_HALF_UP) + "%");
//        JavaPairRDD<String, Integer> counts = testImages.mapToPair((com.gokhanozg.Image image) -> {
//            JavaRDD<com.gokhanozg.CloseImage> closeImages = trainImages.map(trainImage -> {
//                BigDecimal error = BigDecimal.ZERO;
//                String label = trainImage.label;
//                for (int i = 0; i < 28; i++) {
//                    for (int j = 0; j < 28; j++) {
//                        int differ = trainImage.getPixels()[i][j] - image.pixels[i][j];
//                        error = error.add(BigDecimal.valueOf(differ * differ)); // Square of error
//                    }
//                }
//                com.gokhanozg.CloseImage ci = new com.gokhanozg.CloseImage();
//                ci.setError(error);
//                ci.setLabel(label);
//                return ci;
//            });
//            com.gokhanozg.CloseImage closestImage = closeImages.reduce((c1, c2) -> {
//                if (c1.getError().compareTo(c2.getError()) == -1) {
//                    return c1;
//                } else {
//                    return c2;
//                }
//            });
//            if (closestImage.getLabel().equals(image.getLabel())) {
//                //correct guess
//                return new Tuple2<>("c", 1);
//            } else {
//                return new Tuple2<>("w", 1);
//            }
//        }).reduceByKey((a, b) -> a + b);
//        counts.saveAsTextFile(Long.valueOf(System.currentTimeMillis()).toString() + ".mnistData");


//        String largeFilePath = "/home/mephala/Desktop/large.txt";
//        String outputFile = "/home/mephala/Desktop/out";
//
////        JavaRDD<String> textFile = sc.textFile("hdfs://...");
//        JavaRDD<String> textFile = sc.textFile(largeFilePath);
//        JavaPairRDD<String, Integer> counts = textFile
//                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word, 1))
//                .reduceByKey((a, b) -> a + b);
//        counts.saveAsTextFile(outputFile);
//        counts.saveAsTextFile("hdfs://...");

    }
}
