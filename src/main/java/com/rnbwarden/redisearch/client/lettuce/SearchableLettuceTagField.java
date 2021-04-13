package com.rnbwarden.redisearch.client.lettuce;

import java.util.function.Function;

import com.redislabs.lettusearch.Field.Tag;
import com.rnbwarden.redisearch.client.SearchableTagField;

public class SearchableLettuceTagField<E> extends SearchableLettuceField<E> implements SearchableTagField {

    public SearchableLettuceTagField(String name,
                                     boolean sortable,
                                     Function<E, String> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, Tag.builder(name).sortable(sortable).build());
    }
}
