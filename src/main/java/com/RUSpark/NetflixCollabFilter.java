package com.RUSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

/* any necessary Java packages here */

public class NetflixCollabFilter {
	private static final Pattern SPACE = Pattern.compile(",");
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
	private static Tuple2<Integer, Tuple2<Integer,Integer>> getUserData(List<String> s){
		   
		int key = Integer.parseInt(s.get(1));
		Tuple2<Integer,Integer> value = new Tuple2<>(Integer.parseInt(s.get(0)), Integer.parseInt(s.get(2)));
		return new Tuple2<>(key, value);
	  }
	
	private static Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Integer>> generatePair(Tuple2<Integer, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Integer>>> pair){
		int userID = pair._1();
		int movieID = pair._2()._1()._1();
		double rating = pair._2()._1()._2();
		double mean = pair._2()._2()._1();
		int count = pair._2()._2()._2();
		
		return new Tuple2<>(new Tuple2<>(userID, movieID), new Tuple2<>(rating - mean, count));
	}
	
	private static Tuple2<Tuple2<Integer, Integer>, Tuple5<Double, Double, Double, Integer, Integer>> mapToDotNorm(Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Integer>>> pair){
		double dot = 0;
		if (pair._1()._1()._2() == pair._2()._1()._2()) // if 2 movie ids are equal
			dot = pair._1()._2()._1() * pair._2()._2()._1();
		
		int userID1 = pair._1()._1()._1();
		int userID2 = pair._2()._1()._1();
		double norm1 = pair._1()._2()._1();
		double norm2 = pair._2()._2()._1();
		int count1 = pair._1()._2()._2();
		int count2 = pair._2()._2()._2();
		
		return new Tuple2<>(new Tuple2<>(userID1, userID2), new Tuple5<>(dot, norm1, norm2, count1, count2));
		
	}

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixCollabFilter <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			      .builder()
			      .appName("NetflixCollabFilter")
			      .getOrCreate();

	    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

	    JavaRDD<List<String>> lists = lines.map(s -> splitLineInfo(s));
	    
	    JavaPairRDD<Integer, Tuple2<Integer, Integer>> userRatings = lists.mapToPair(s -> getUserData(s));
	    JavaPairRDD<Integer, Tuple2<Double, Integer>> aggregatedRating = userRatings.aggregateByKey(
	    		new Tuple2<Double, Integer>(0.0, 0), 
	    		(agg, rating) -> new Tuple2<>(agg._1() + rating._2(), agg._2() + 1),
	    		(agg1, agg2) -> new Tuple2<>(agg1._1() + agg2._1(), agg1._2() + agg2._2()));
	    		
	    
	    JavaPairRDD<Integer, Tuple2<Double, Integer>> meanRating = aggregatedRating.mapToPair(s -> new Tuple2<>(s._1(), new Tuple2<>(s._2()._1() / s._2()._2(), s._2()._2())));
	    
	    
	    JavaPairRDD<Tuple2<Integer, Integer>,Tuple2<Double, Integer>> ratingPair = userRatings.join(meanRating)
	    		.mapToPair(rating -> generatePair(rating))
	    		.filter(pair -> pair._2()._1() != 0.0);
		
//		List<Tuple2<Tuple2<Integer,Integer>, Tuple2<Double, Double>>> col = ratingPair.collect();
//		
//		for (int i = 0; i < col.size(); i++) {
//			System.out.println("userId, movieId, rating-mean, mean: " + col.get(i)._1()._1() + " " + col.get(i)._1()._2() + " " + col.get(i)._2()._1() + " " + col.get(i)._2()._2());			
//		}
	    
	    JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Integer>>> joinedPairs = ratingPair.cartesian(ratingPair)
	    		.filter(row -> row._1()._1()._1() < row._2()._1()._1());
	 
	    
		List<Tuple2<Tuple2<Tuple2<Integer,Integer>, Tuple2<Double, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Integer>>>> col = joinedPairs.collect();
		
//		for (int i = 0; i < col.size(); i++) {
//			System.out.println("paired row: userID, movieID, rating-mean, count " 
//						+ "(" + col.get(i)._1()._1()._1() + "," + col.get(i)._1()._1()._2() + "," + col.get(i)._1()._2()._1() + "," + col.get(i)._1()._2()._2() + ") "
//						+ "(" + col.get(i)._2()._1()._1() + "," + col.get(i)._2()._1()._2() + "," + col.get(i)._2()._2()._1() + "," + col.get(i)._2()._2()._2() + ")");
//		}
		
		
	    JavaPairRDD<Tuple2<Integer, Integer>, Tuple5<Double, Double, Double, Integer, Integer>> 
	    		calc = joinedPairs.mapToPair(pair -> mapToDotNorm(pair))
	    						  .aggregateByKey(
	    								  new Tuple5<>(0.0, 0.0, 0.0, 0, 0),
	    								  (agg, val) -> new Tuple5<>(agg._1() + val._1(), agg._2() + val._2()*val._2(), agg._3() + val._3()*val._3(), val._4(), val._5()),
	    								  (agg1, agg2) -> new Tuple5<>(agg1._1() + agg2._1(), agg1._2() + agg2._2(), agg1._3() + agg2._3(), agg1._4(), agg1._5()));
	    
//	    for (Tuple2<Tuple2<Integer, Integer>, Tuple5<Double, Double, Double, Integer, Integer>> item : calc.collect()) {
//	    	System.out.println("user pair: " + "(" + item._1()._1() + "," + item._1()._2() + "): "
//	    			+ "" + "(dot, norm1, norm2, count1, count2): " + item._2()._1() + " " + item._2()._2() + " " + item._2()._3() + " " + item._2()._4() + " " + item._2()._5());
//	    }

	    JavaPairRDD<Tuple2<Integer, Integer>, Double> simRDD = calc.mapToPair(val -> 
	    			new Tuple2<>(new Tuple2<>(val._1()._1(), val._1()._2()), 
	    					(double) val._2()._1() / Math.sqrt((val._2()._2() / val._2()._5()) * (val._2()._3() / val._2()._4()))));
	    
//	    List<Tuple2<Tuple2<Integer, Integer>, Double>> sim = simRDD.collect();
//	    
//	    for (int i = 0; i < sim.size(); i++) {
//	    	System.out.println("(" + sim.get(i)._1()._1() + "," + sim.get(i)._1()._2() + "): " + sim.get(i)._2());
//	    }
	    
	    JavaPairRDD<Integer, Tuple2<Integer, Double>> splittedSimRDD = simRDD.flatMapToPair(pair -> {
	        int u = pair._1()._1();
	        int v = pair._1()._2();
	        double x = pair._2();

	        List<Tuple2<Integer, Tuple2<Integer, Double>>> result = new ArrayList<>();
	        
	        // Emit a pair for user u: (u, (v, x))
	        result.add(new Tuple2<>(u, new Tuple2<>(v, x)));

	        // Emit a pair for user v: (v, (u, x))
	        result.add(new Tuple2<>(v, new Tuple2<>(u, x)));

	        return result.iterator();
	    });
	    
	    
	    // Sort to get top k values for each userID
	    int k = 11;
	    
	    JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> topKValues = splittedSimRDD.groupByKey().mapValues(
	            values -> {
	                // Use Java 8 Stream API to sort and limit the values
	                return StreamSupport.stream(values.spliterator(), false)
	                        .sorted((o1, o2) -> Double.compare(o2._2(), o1._2())) // Sort in descending order
	                        .limit(k)
	                        .collect(Collectors.toList());
	            }
	    );
	    
	    // Print the top k values for each userID
//	    topKValues.collect().forEach(System.out::println);
	    
	    int M = 3;
	    
	 // Extract top M movies for each user
	    JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> topMRatings = topKValues.flatMapValues(
	    	    similarityList -> {
	    	        // Extract movie ratings for each user
	    	        List<Tuple2<Integer, Integer>> userRatingsList = new ArrayList<>();
	    	        for (Tuple2<Integer, Double> similarityTuple : similarityList) {
	    	            int otherUserId = similarityTuple._1();

	    	            // Filter userRatings to get ratings for the other user
	    	            List<Tuple2<Integer, Integer>> otherUserRatings = userRatings.filter(
	    	                    userRating -> userRating._1().equals(otherUserId)
	    	            ).map(
	    	                    userRating -> new Tuple2<>(userRating._2()._1(), userRating._2()._2())
	    	            ).collect();
	    	            
	    	            List<Tuple2<Integer, Integer>> topMRatingsList = otherUserRatings.stream()
		    	                .sorted((o1, o2) -> Integer.compare(o2._2(), o1._2())) // Sort in descending order based on rating
		    	                .limit(M)
		    	                .collect(Collectors.toList());

	    	            // Add the other user's ratings to the list
	    	            userRatingsList.addAll(otherUserRatings);
	    	        }

	    	        // Return an Iterable containing the top M ratings for each user
	    	        return (Iterator<Iterable<Tuple2<Integer, Integer>>>) Collections.singletonList(userRatingsList);
	    	    }
	    	);

	    // Print the top M movie ratings for each user
	    List<Tuple2<Integer, Iterable<Tuple2<Integer,Integer>>>> getList = topMRatings.collect();
	    
	    for (Tuple2<Integer, Iterable<Tuple2<Integer,Integer>>> item : getList) {
	    	List<Tuple2<Integer,Integer>> movies = (List<Tuple2<Integer, Integer>>) item._2();
	    	System.out.print(item._1() + ": movie list - ");
	    	for (int i = 0; i < movies.size(); i++)
	    		System.out.print(movies.get(i)._2() + " ");
	    	System.out.println();
	    }
	    
	}

}
