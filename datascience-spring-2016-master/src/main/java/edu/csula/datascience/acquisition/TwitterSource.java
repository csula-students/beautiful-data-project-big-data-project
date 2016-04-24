package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Collection;
import java.util.List;

/**
 * An example of Source implementation using Twitter4j api to grab tweets
 */
public class TwitterSource implements Source<Status> {
    private String minId;
    private final String searchQuery;

    public TwitterSource(String minId, String query) {
        this.minId = minId;
        this.searchQuery = query;
    }

    @Override
    public boolean hasNext() {
        return minId.length() > 0;
    }

    @Override
    public Collection<Status> next() {
        List<Status> list = Lists.newArrayList();
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey("J1Q2Q9TbwZIWEqTfAghClw9Tb")
            .setOAuthConsumerSecret("IqEvLwkHUgNcjwZLIeuS6n40z0mKW3UYOLKujqhqzD1nOOxm0i")
            .setOAuthAccessToken("443691458-PNOLBJRMMObtNxchZ3XGHqAEcNlJABsW70XglEMv")
            .setOAuthAccessTokenSecret("L1kybDJeVdwy7bWvh47B2oQ5rIZFi7Vz1NSq8xpQLztAl");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        Query query = new Query(searchQuery);
        query.setLang("EN");
        query.setSince("20140101");
//        if (minId != Long.MAX_VALUE) {
//            query.setMaxId(minId);
//        }

        list.addAll(getTweets(twitter, query));

        return list;
    }

    private List<Status> getTweets(Twitter twitter, Query query) {
        QueryResult result;
        List<Status> list = Lists.newArrayList();
        try {
            do {
                result = twitter.search(query);

                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                  //  minId = Math.min(minId, tweet.getId());
                	minId = String.valueOf(tweet.getId());
                }
                list.addAll(tweets);
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException e) {
            // Catch exception to handle rate limit and retry
            e.printStackTrace();
            System.out.println("Got twitter exception. Current min id " + minId);
            try {
                Thread.sleep(e.getRateLimitStatus()
                    .getSecondsUntilReset() * 1000);
                list.addAll(getTweets(twitter, query));
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }

        return list;
    }
}
