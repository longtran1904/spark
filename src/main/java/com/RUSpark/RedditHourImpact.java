package com.RUSpark;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditHourImpact {
	private static final Pattern SPACE = Pattern.compile(",");
	  
	  private static Tuple2<Integer, Integer> calculateImpactHour(List<String> s){
//		  Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT-5"));
		  LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(s.get(1))), ZoneId.systemDefault());

	      // Get the hour from LocalDateTime
	      int key = dateTime.getHour();

//		  calendar.setTimeInMillis(Integer.parseInt(s.get(1)) * 1000L);
		  int value = Integer.parseInt(s.get(4)) + Integer.parseInt(s.get(5)) + Integer.parseInt(s.get(6));
//		  System.out.println("unixtime: " + Integer.parseInt(s.get(1)) + 
//				  "; (hour: " + key + ", impact: " + value + ")");
		  return new Tuple2<>(key, value);
	  }
	  
	  private static List<String> splitLineInfo(String s) {
		  List<String> result;
		  result = new ArrayList<>(Arrays.asList(SPACE.split(s)));
		  while (result.size() > 7) {
			  String i = result.get(2);
			  String j = result.get(3);
			  i = i.concat("," + j);
			  result.set(2, i);
			  result.remove(3);
		  }
		  return result;
	  }

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			      .builder()
			      .appName("RedditHourImpact")
			      .getOrCreate();
	
			    			JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
	
	    JavaRDD<List<String>> lists = lines.map(s -> splitLineInfo(s));

	    JavaPairRDD<Integer, Integer> hours = lists.mapToPair(s -> calculateImpactHour(s));
	    
//	    System.out.println("Each line info");
//	    for (Tuple2<?,?> tuple: hours.collect()) {
//	    	System.out.println(tuple._1() + " " + tuple._2());
//	    }

	    JavaPairRDD<Integer, Integer> sum_image = hours.reduceByKey((i1, i2) -> i1 + i2);

//			    System.out.println("Each image impact");
	    List<Tuple2<Integer, Integer>> output = sum_image.sortByKey(true).collect();
	    for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + " " + tuple._2());
	    }
			
	    spark.stop();							
	}

}
