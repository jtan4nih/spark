package com.matthewrathbone.sparktest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.matthewrathbone.sparktest.cassandra.CassandraConnection;

public class ExampleJob {
    private static JavaSparkContext sc;
    
    public ExampleJob(JavaSparkContext sc){
    	this.sc = sc;
    }
    
    public static final PairFunction<Tuple2<String, Optional<String>>, String, String> KEY_VALUE_PAIRER =
    new PairFunction<Tuple2<String, Optional<String>>, String, String>() {
    	public Tuple2<String, String> call(
    			Tuple2<String, Optional<String>> a) throws Exception {
			// a._2.isPresent()
    		if(a._2.isPresent()) {
    			return new Tuple2<String, String>(a._1, a._2.get());
    		} else {
    			return new Tuple2<String, String>(a._1, ""); // this temp for the fix of  [java.lang.IllegalStateException: Optional.get() cannot be called on an absent value]
    		}
    	}
	};
	
	public static JavaRDD<Tuple2<String,Optional<String>>> joinData(JavaPairRDD<String, String> t, JavaPairRDD<String, String> u){
        JavaRDD<Tuple2<String,Optional<String>>> leftJoinOutput = t.leftOuterJoin(u).values().distinct();
        return leftJoinOutput;
	}
	
	public static JavaPairRDD<String, String> modifyData(JavaRDD<Tuple2<String,Optional<String>>> d){
		return d.mapToPair(KEY_VALUE_PAIRER);
	}
	
	public static Map<String, Object> countData(JavaPairRDD<String, String> d){
        Map<String, Object> result = d.countByKey();
        return result;
	}
	
	public static JavaPairRDD<String, String> run(String reviewTransPath, String productDetailsPath){
		CassandraConnection cassandraConnection = CassandraConnection.getInstance();
		reviewTransPath = cassandraConnection.getReviewTransactions();
		productDetailsPath = cassandraConnection.getProductDetails();
        JavaRDD<String> transactionInputFile = sc.textFile(reviewTransPath);
        JavaPairRDD<String, String> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] transactionSplit = s.split("\t");
                return new Tuple2<String, String>(String.valueOf(transactionSplit[2]), String.valueOf(transactionSplit[1]));
            }
        });
        
        JavaRDD<String> customerInputFile = sc.textFile(productDetailsPath);
        JavaPairRDD<String, String> productDetails = customerInputFile.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] productDetailsSplit = s.split("\t");
                return new Tuple2<String, String>(String.valueOf(productDetailsSplit[0]), productDetailsSplit[3]);
            }
        });

        Map<String, Object> result = countData(modifyData(joinData(transactionPairs, productDetails)));
        
        List<Tuple2<String, String>> output = new ArrayList<>();
	    for (Entry<String, Object> entry : result.entrySet()){
	    	output.add(new Tuple2<>(entry.getKey().toString(), String.valueOf((long)entry.getValue())));
	    }

	    JavaPairRDD<String, String> output_rdd = sc.parallelizePairs(output);
	    return output_rdd;
	}
	
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));
        ExampleJob job = new ExampleJob(sc);
        JavaPairRDD<String, String> output_rdd = job.run(args[0], args[1]);
        output_rdd.saveAsHadoopFile(args[2], String.class, String.class, TextOutputFormat.class);
        sc.close();
    }
}
