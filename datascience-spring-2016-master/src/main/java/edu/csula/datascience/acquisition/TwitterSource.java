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
    private long minId;
    private final String searchQuery;

    public TwitterSource(long minId, String query) {
        this.minId = minId;
        this.searchQuery = query;
    }

    @Override
    public boolean hasNext() {
        return minId > 0;
    }

    @Override
    public Collection<Status> next() {
        List<Status> list = Lists.newArrayList();
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey("UiwZcTUP7hCIdSvpRSsZSWhbS")
            .setOAuthConsumerSecret("9u0gOM2cFSaqGwRJmhQQa4z4Cbq15lyydq0BjxvbbaFwVg0Kdp")
            .setOAuthAccessToken("723735972538535936-pzCe18UQym2ub3F5BFJmj9ubbIaezEO")
            .setOAuthAccessTokenSecret("kSMQDPZS5XM4URdwRqmuw5rMzeNFKRuoJ3wkaxGnzJVdt");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        Query query = new Query(searchQuery);
        query.setLang("EN");
        query.setSince("20140101");
        if (minId != Long.MAX_VALUE) {
            query.setMaxId(minId);
        }

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
                	System.out.println(tweet.getText());
                    minId = Math.min(minId, tweet.getId());
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
