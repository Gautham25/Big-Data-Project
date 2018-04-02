package edu.ucr.cs.cs226.GroupG;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


public class TrendEstimator {

    public static JavaRDD<Tweet> load(String tweetsFile, County c, JavaSparkContext sc) {

        //read tweets file
        JavaRDD<String> lines = sc.textFile(tweetsFile);

        JavaRDD<Tweet> myTweets = lines.map(line -> {
            JSONParser jsonParser = new JSONParser();
            GeometryFactory gf=new GeometryFactory();

            JSONObject tweetObject = null;
            try {
                tweetObject = (JSONObject) jsonParser.parse(line);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            long tweetId = (long) tweetObject.get("id"); //id
            //String tweetText = (String) tweetObject.get("text"); //text
            JSONArray tArray = (JSONArray) tweetObject.get("filtered_text");

            String dt=(String)tweetObject.get("created_at");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy");
            LocalDateTime dateTime = LocalDateTime.parse(dt, formatter);

            StringBuilder tText = new StringBuilder();
            Iterator arrI = tArray.iterator();
            while (arrI.hasNext()) {
                tText.append((String) arrI.next() + " ");
            }
            String tweetText = " ";
            if (tText.length() > 0)
                tweetText = tText.substring(0, tText.lastIndexOf(" "));
            //System.out.println(tweetText);

            JSONObject userObj = (JSONObject) tweetObject.get("user");  //user
            long userName  = (long) userObj.get("id"); //userId

            JSONArray locArray = new JSONArray();
            if(tweetObject.get("place") !=null) {
                JSONObject placeObj = (JSONObject) tweetObject.get("place");//place
                if(placeObj.get("bounding_box") !=null) {
                    JSONObject tObj = (JSONObject) placeObj.get("bounding_box");//bounding_box
                    JSONArray locObj = (JSONArray) tObj.get("coordinates");//coordinates
                    locArray = (JSONArray) locObj.iterator().next();
                }
            }
            Iterator out = locArray.iterator();
            Coordinate[] points = new Coordinate[5];
            int counter = 0;
            while(out.hasNext()){
                JSONArray outer = (JSONArray) out.next();
                Iterator in = outer.iterator();
                double x=0,y=0;
                int cx=0;
                while(in.hasNext()){
                    String item = in.next().toString();
                    if (cx++ == 0)
                        x = Double.parseDouble(item);
                    else
                        y = Double.parseDouble(item);
                }
                points[counter++] = new Coordinate(x, y);
            }
            points[4] = points[0];
            //check
            boolean check = true;
            for(int k=0; k<points.length; k++){
                if(points[k] == null)
                    check = false;
            }
            String countyForTweet = "outside USA";
            if(check) {
                Geometry sg = gf.createPolygon(points);
                Geometry center = sg.getCentroid();
                Geometry tmp;
                for (int in = 0; in < c.geom.size(); in++) {
                    tmp = c.geom.get(in);
                    if (center.coveredBy(tmp)) {
                        countyForTweet = c.name.get(in);
                        break;
                    }
                }
            }
            return new Tweet(tweetId, tweetText, userName, countyForTweet,dateTime);
        }); //end map
        return myTweets;
    }

    public static JavaPairRDD<String, Long> getCount (JavaRDD<Tweet> tw){
        JavaPairRDD<String, Long> wordCount = tw
                .flatMap(t -> Arrays.asList(t.tweetText.split(" ")).iterator() )
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((a, b) -> a + b);
        return wordCount;
    }

    public static List<String> estimate(JavaRDD<Tweet> myTweets) {
        JavaPairRDD<String, Long> wordCount = getCount(myTweets);

        JavaPairRDD<String, HashSet<Long>> userCount = myTweets.flatMapToPair(t ->{
            String[] words = t.tweetText.split(" ");
            ArrayList<Tuple2<String, HashSet<Long>>> l = new ArrayList<>();
            HashSet<Long> u = new HashSet<>();
            u.add( (long) t.userId  );
            for(String w: words){
                l.add(new Tuple2(w, u) );
            }
            return l.iterator();
        });

        JavaPairRDD<String, HashSet<Long>> uniqueUsers = userCount.reduceByKey((a, b) -> {
            for(long i : b){
                a.add( i);
            }
            return a;
        });

        Map<String, Long> tw = wordCount.collectAsMap();
        HashMap<String, Long> words = new HashMap<String, Long>(tw);

        Map<String, HashSet<Long>> tu = uniqueUsers.collectAsMap();
        HashMap<String, HashSet<Long>> users = new HashMap<String, HashSet<Long>>(tu);

        HashMap<String, Double> trends = new HashMap<>();

        for ( String key : words.keySet() ) {
            trends.put(key, Math.log10(words.get(key) ) * users.get(key).size());
        }

        Set<Map.Entry<String, Double>> set = trends.entrySet();
        List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(
                set);
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1,
                    Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        List<String> top = new ArrayList<>();
        int c=0;
        for (Map.Entry<String, Double> entry : list) {
            if(c++ < 10) {
                //System.out.println(entry.getKey()+":"+entry.getValue());
                top.add(entry.getKey());
            }

        }

        return top;
    }

    public static County loadCounty(String countyFile) {
        ArrayList<Geometry> countyGeom = new ArrayList<>();
        ArrayList<String> countyName = new ArrayList<>();


        //read county data
        FileReader r;
        BufferedReader buf;
        try{
            r = new FileReader(countyFile);
            buf = new BufferedReader(r);
            String line;
            while ((line = buf.readLine()) != null) {
                String parts[] = line.split("\t");
                String coord = parts[0].replace("\"", "");
                Geometry geom = null;
                try {
                    // Parse string as well known text (WKT)
                    final WKTReader wktReader = new WKTReader();
                    geom = wktReader.read(coord);
                } catch (com.vividsolutions.jts.io.ParseException e) {
                    try {
                        // Error parsing from WKT, try hex string instead
                        byte[] binary = WKBReader.hexToBytes(coord);
                        final WKBReader wkbReader = new WKBReader();
                        geom = wkbReader.read(binary);
                    } catch (RuntimeException e1) {
                        // Cannot parse text. Just return null
                    } catch (com.vividsolutions.jts.io.ParseException e1) {
                        e1.printStackTrace();
                    }
                }
                countyGeom.add(geom);
                countyName.add(parts[5]);

            }//end while
        }
        catch (IOException e){
            System.out.println("Error in reading COUNTY file");
        }
        return new County(countyGeom, countyName);
    }

    public static JavaRDD<Tweet> filterForCounty(JavaRDD<Tweet> tweets, String reqCounty) {
        return tweets.filter(t -> t.county.toLowerCase().equals(reqCounty) );
    }

    public static JavaRDD<Tweet> filterForTime(JavaRDD<Tweet> tweets, LocalDateTime starttime, LocalDateTime endtime) {
        return tweets.filter(t -> t.dateTime.compareTo(starttime)>=0 && t.dateTime.compareTo(endtime)<=0);
    }

    public static JavaRDD<Long> getUsersForCounty(JavaRDD<Tweet> tweets) {
        JavaRDD<Long> allUsers = tweets.map(t -> t.userId);
        return allUsers.distinct();
    }

    public static Map<Long,List<String>> estimateForNetwork(JavaRDD<Tweet> tweets, String networkFile, List<Long> users,
            JavaSparkContext sc) {
        JavaRDD<String> relationships = sc.textFile(networkFile);
        Map<Long,List<String>> uTrends = new HashMap<>();
        //for each user in the set
        for(long u : users){
            //get network of user
            JavaRDD<Long> uNetwork = getNetwork(u, relationships);
            //System.out.println("No. of users in network of "+u+ " "+uNetwork.count());
            long nsize = uNetwork.count();
            if(nsize > 10) {
                JavaRDD<Tweet> networkTweets = getTweetsForNetwork(tweets, uNetwork);
                //System.out.println("Number of Tweets in Network: " + networkTweets.count());
                long nwtsize = networkTweets.count();
                if(nwtsize > 100) {
                    List trends = estimate(networkTweets);
                    uTrends.put(u, trends);
                }
            }
        }
        return uTrends;
    }

    private static JavaRDD<Tweet> getTweetsForNetwork(JavaRDD<Tweet> tweets, JavaRDD<Long> uNetwork) {
        List<Long> network = uNetwork.collect();
        return tweets.filter(t -> network.contains(t.userId) );
    }

    private static JavaRDD<Long> getNetwork(long u, JavaRDD<String> network) {
        //filter
        JavaRDD<String> filterednetwork = network.filter(line ->{
            String rel[] = line.split("\t");
            return Long.parseLong(rel[0]) == u || Long.parseLong(rel[1]) == u;
        });

        JavaRDD<Long> userNetwork = filterednetwork.flatMap(line -> {
            String[] tmpU = line.split("\t");
            List all = new ArrayList<Long>(){{
                add(Long.parseLong(tmpU[0]));
                add(Long.parseLong(tmpU[1]));
            }};

            return all.iterator();
        });
        return userNetwork.distinct();
    }

    public static Map<Long,List<String>> estimateForUser(JavaRDD<Tweet> tweets, List<Long> users, JavaSparkContext sc) {

        Map<Long,List<String>> userTrends = new HashMap<>();

        //for each user in the set
        for(long u : users){
            JavaRDD<Tweet> userTweets = getTweetsForUser(tweets, u);
            List trends = estimateForSingleUser(userTweets);
            userTrends.put(u, trends);
        }
        return userTrends;
    }

    private static List estimateForSingleUser(JavaRDD<Tweet> userTweets) {
        JavaPairRDD<String, Long> wordCount = getCount(userTweets);
        Map<String, Long> tw = wordCount.collectAsMap();
        HashMap<String, Long> words = new HashMap<String, Long>(tw);

        Set<Map.Entry<String, Long>> set = words.entrySet();
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(
                set);
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1,
                    Map.Entry<String, Long> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        List top = new ArrayList<String>();
        int c=0;
        for (Map.Entry<String, Long> entry : list) {
            if(c++ < 10) {
                //System.out.println(entry.getKey()+":"+entry.getValue());
                top.add(entry.getKey());
            }
            else
                break;

        }

        return top;
    }

    private static JavaRDD<Tweet> getTweetsForUser(JavaRDD<Tweet> tweets, long u) {
        return tweets.filter(t->t.userId==u);
    }

    public static List<Long> getTopUsersForCounty(JavaRDD<Tweet> tweetsForCounty, JavaSparkContext sc) {

        JavaPairRDD<Long, Long> uPairs = tweetsForCounty.mapToPair(t -> new Tuple2(t.userId, 1L) );
        JavaPairRDD<Long, Long> uFreq = uPairs.reduceByKey((a, b) -> a + b);

        Map<Long, Long> ut = uFreq.collectAsMap();
        HashMap<Long, Long> uMap = new HashMap<Long, Long>(ut);

        Set<Map.Entry<Long, Long>> set = uMap.entrySet();
        List<Map.Entry<Long, Long>> list = new ArrayList<Map.Entry<Long, Long>>(
                set);
        Collections.sort(list, new Comparator<Map.Entry<Long, Long>>() {
            public int compare(Map.Entry<Long, Long> o1,
                    Map.Entry<Long, Long> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        List topUsers = new ArrayList<Long>();
        int c=0;
        for (Map.Entry<Long, Long> entry : list) {
            if(c++ < 10) {
                //System.out.println(entry.getKey()+":"+entry.getValue());
                topUsers.add(entry.getKey());
            }
            else
                break;
        }

        return topUsers;

    }
}
