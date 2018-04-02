package edu.ucr.cs.cs226.GroupG;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;

public class Twitter_clean {

    public static void main( String[] args ) {
        SparkSession session = SparkSession.builder().master("local").appName("jsonreader").getOrCreate();
        Dataset<Row> tweet_data = session.read().json("../sample_tweets/cleaned_tweets.json");

        Tokenizer tokenizer = new Tokenizer().setInputCol("textclean").setOutputCol("words");
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("textclean")
                .setOutputCol("words")
                .setPattern("\\W");

        Dataset<Row> tokenized = tokenizer.transform(tweet_data);
        tokenized.select("textclean", "words")
                .show(false);

        Dataset<Row> regexTokenized = regexTokenizer.transform(tweet_data);
        regexTokenized.select("textclean", "words")
                .show(false);
        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered_text");
        Dataset<Row> output=remover.transform(regexTokenized);

        output.write().json("../sample_tweets/processed_tweets.json");
        session.stop();
    }

}
