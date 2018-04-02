package edu.ucr.cs.cs226.GroupG;

import scala.collection.Iterable;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Tweet implements Serializable{
    public long tweetId;
    public String tweetText; //text
    public long userId;
    public String county;
    public LocalDateTime dateTime;

    public Tweet(long tweetId, String tweetText, long userName, String county, LocalDateTime datetime) {
        this.tweetId = tweetId;
        this.tweetText = tweetText;
        this.userId = userName;
        this.county = county;
        this.dateTime=datetime;
    }

    public Tweet() {
    }
}
