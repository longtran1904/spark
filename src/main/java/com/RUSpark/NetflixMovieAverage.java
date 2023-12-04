package com.RUSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {
	private static final Pattern SPACE = Pattern.compile(",");
	  
	  private static Tuple2<Integer, Integer> calculateRating(List<String> s){
		  int key = Integer.parseInt(s.get(0));
		  int value = Integer.parseInt(s.get(2));
		  return new Tuple2<>(key, value);
	  }
	  
	  private static List<String> splitLineInfo(String s) {
		  List<String> result;
		  result = new ArrayList<>(Arrays.asList(SPACE.split(s)));
//		  while (result.size() > 7) {
//			  String i = result.get(2);
//			  String j = result.get(3);
//			  i = i.concat("," + j);
//			  result.set(2, i);
//			  result.remove(3);
//		  }
		  return result;
	  }

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			      .builder()
			      .appName("NetflixMovieAverage")
			      .getOrCreate();

	    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

	    JavaRDD<List<String>> lists = lines.map(s -> splitLineInfo(s));
	    
//			    for (List<String> list: lists.collect()) {
//			    	System.out.println("(" + list.get(0) + ", " + list.get(4) + ", " + list.get(5) + ", " + list.get(6) + "): "
//			    			+ "title: " + list.get(2));
//			    }

	    JavaPairRDD<Integer, Integer> moviesRating = lists.mapToPair(s -> calculateRating(s));
	    JavaPairRDD<Integer, Integer> moviesRatingCount = lists.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.get(0)), 1));
	    
	    
//			    System.out.println("Each line info");
//			    for (Tuple2<?,?> tuple: images.collect()) {
//			    	System.out.println(tuple._1() + " " + tuple._2());
//			    }

	    JavaPairRDD<Integer, Integer> sumRating = moviesRating.reduceByKey((i1, i2) -> i1 + i2);
	    JavaPairRDD<Integer, Integer> movieRatingCount = moviesRatingCount.reduceByKey((i1, i2) -> i1 + i2);
	    
	    JavaPairRDD<Integer, Tuple2<Integer,Integer>> joinedData = sumRating.join(movieRatingCount);
	    
	    JavaPairRDD<Integer, Float> AvgRating = joinedData.mapValues(e -> (float) e._1() / e._2());
	    
//			    System.out.println("Each image impact");
	    List<Tuple2<Integer, Float>> output = AvgRating.sortByKey(true).collect();
	    for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + " " + tuple._2());
	    }
			
	    spark.stop();
		
	}

}
