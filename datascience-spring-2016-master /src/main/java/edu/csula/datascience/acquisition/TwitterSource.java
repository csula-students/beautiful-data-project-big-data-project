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
            .setOAuthConsumerKey("uNU4y23okvTUziCJ2cQyXbaGV")
            .setOAuthConsumerSecret("fR6EctAITAxtxTjylsYbhMWmAnN8VRiCxzbt9uhXYlPPRxueGs")
            .setOAuthAccessToken("2482703839-3sBYf0YF0yWHpr9DDlUKsu1sOFNfONe2T88KLPF")
            .setOAuthAccessTokenSecret("AzoRG3H1hyrlgUmWgAuokJWGwtlaw5zyxKoO22IYFoABM");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        
        List<Status> status = null;

        try {

        status = twitter.getHomeTimeline();

        } catch (TwitterException e) {

        // TODO Auto-generated catch block

        e.printStackTrace();

        }

                for(Status st: status)

                {

                System.out.println(st.getUser().getDescription()+"------"+st.getUser().getName()+"-------"+st.getText());

                }

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
