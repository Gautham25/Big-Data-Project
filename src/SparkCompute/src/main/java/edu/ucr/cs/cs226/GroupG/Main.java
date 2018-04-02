package edu.ucr.cs.cs226.GroupG;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.LocalDateTime;

import java.util.List;
import java.util.Map;

/*
Command line arguments:
1: String: full path for tweets (in json format)
2: String: county file (downloaded from spatial datasets web)
3: String: Name of county
4: String: follow relationship of user; a \t b means 'a' follows 'b'
5: LocalDateTime: start time for window. For example, 2018-01-29T00:00:00
6: LocalDateTime: end time for window. For example, 2018-01-30T00:00:00
 */

public class Main {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Spark Compute Engine for Twitter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String tweetsFile = null, countyFile=null, reqCounty=null, networkFile=null;
        LocalDateTime starttime=LocalDateTime.now(),endtime=LocalDateTime.now();
        if (args.length >= 6) {
            tweetsFile = args[0];
            countyFile = args[1];
            reqCounty = args[2].toLowerCase();
            networkFile = args[3];
            starttime=LocalDateTime.parse(args[4]);
            endtime=LocalDateTime.parse(args[5]);

        }
        //load county data
        County county = TrendEstimator.loadCounty(countyFile);
        //load all tweets
        JavaRDD<Tweet> tweets = TrendEstimator.load(tweetsFile, county, sc);
        //System.out.println(tweets.count());
        //filter tweets based on time
        JavaRDD<Tweet> tweetsbyTime = TrendEstimator.filterForTime(tweets, starttime, endtime);
        //get tweets for county, passed by user
        JavaRDD<Tweet> tweetsForCounty = TrendEstimator.filterForCounty(tweetsbyTime, reqCounty);
        //System.out.println("Number of Tweets for County: "+ tweetsForCounty.count());
        //get trends for county, passed by user
        List<String> trends = TrendEstimator.estimate(tweetsForCounty);
        //System.out.println(trends.size());

        //write results to a file
        FileWriter fw = new FileWriter("results_"+reqCounty+".txt");
        BufferedWriter bufferedWriter=new BufferedWriter(fw);
        PrintWriter pw = new PrintWriter(bufferedWriter);

        //System.out.println("Top Trends for county: "+reqCounty);
        pw.println("Top Trends for county: "+reqCounty);
        for(String s: trends) {
            System.out.println(s);
            pw.println(s);
        }
        pw.println("---------------------");
        pw.flush();

        //get list of users for county, passed by user
        //JavaRDD<Long> users = TrendEstimator.getUsersForCounty(tweetsForCounty);
        List<Long> users = TrendEstimator.getTopUsersForCounty(tweetsForCounty, sc);
        //System.out.println("Number of users : "+users.size());
        //get trend for each user's network
        Map<Long, List<String>> networkTrend = TrendEstimator.estimateForNetwork(tweetsbyTime, networkFile, users, sc);
        //System.out.println("Top Trends for users' network: "+reqCounty);
        pw.println("Top Trends for users' network: "+reqCounty);
        for (Map.Entry<Long,List<String>> entry : networkTrend.entrySet()){
            //System.out.println("User: "+entry.getKey()+" network");
            pw.println("User: "+entry.getKey()+" network");
            List<String> ut = entry.getValue();
            for(String s: ut) {
                System.out.println(s);
                pw.println(s);
            }
        }
        pw.println("---------------------");
        pw.flush();

            //get trend for each user
        Map<Long,List<String>> userTrend=TrendEstimator.estimateForUser(tweetsbyTime,users,sc);

        //System.out.println("Top Trends for users: "+reqCounty);
        pw.println("Top Trends for users: "+reqCounty);
        for (Map.Entry<Long,List<String>> entry : userTrend.entrySet()){
            //System.out.println("User: "+entry.getKey());
            pw.println("User: "+entry.getKey());
            List<String> ut = entry.getValue();
            for(String s: ut) {
                System.out.println(s);
                pw.println(s);
            }
        }
        pw.flush();
        pw.close();
        bufferedWriter.close();
        fw.close();
    }//main
}
