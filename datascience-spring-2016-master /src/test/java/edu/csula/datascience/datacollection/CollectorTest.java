package edu.csula.datascience.datacollection;

import com.google.common.collect.Lists;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;

/**
 * A test case to show how to use Collector and Source
 */
public class CollectorTest {
    private Collector<SimpleModel, MockData> collector;
    private Source<MockData> source;

    @Before
    public void setup() {
        collector = new MockCollector();
        source = new MockSource();
    }

    @Test
    public void mungee() throws Exception {
    	
    List<SimpleModel> list = (List<SimpleModel>) collector.mungee(source.next());
        List<SimpleModel> expectedList = Lists.newArrayList(
            new SimpleModel("2", new JSONObject(JSONObject.stringToValue("{name:Wanted}"))),
            new SimpleModel("3", new JSONObject(JSONObject.stringToValue("{name:the walking dead}")))
        );

        Assert.assertEquals(list.size(), 2);

        for (int i = 0; i < 2; i ++) {
            Assert.assertEquals(list.get(i).getId(), expectedList.get(i).getId());
            JSONAssert.assertEquals(list.get(i).getJson(), expectedList.get(i).getJson(),false);
        }
    }
}