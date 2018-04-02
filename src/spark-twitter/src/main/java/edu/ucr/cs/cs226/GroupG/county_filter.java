package edu.ucr.cs.cs226.GroupG;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class county_filter {
    public static void main( String[] args ) {
        SparkConf conf = new SparkConf().setAppName("Spark Compute Engine for Twitter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD <String> index_data = sc.textFile("/home/ssing068/Documents/BigData/Project/spark-twitter/part-r-0000");

    }
}
