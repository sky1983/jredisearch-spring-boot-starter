package com.rnbwarden.redisearch.redis.client;

import java.util.List;

public interface SearchResults {

    Long getTotalResults();

    List<SearchResult<String, Object>> getResults();

    default boolean hasResults() {

        return getTotalResults() > 0;
    }
}