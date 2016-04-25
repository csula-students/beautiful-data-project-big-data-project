package edu.csula.datascience.datacollection;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * A mock implementation of collector for testing
 */
public class MockCollector implements Collector<SimpleModel, MockData> {
    @Override
    public Collection<SimpleModel> mungee(Collection<MockData> src) {
        // in your example, you might need to check src.hasNext() first
        return src
            .stream()
            .filter(data -> data.getJson() != null)
            .map(SimpleModel::build)
            .collect(Collectors.toList());
    }

    @Override
    public void save(Collection<SimpleModel> data) {
    }
}
