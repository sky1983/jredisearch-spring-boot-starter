package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.Field.Text;
import com.rnbwarden.redisearch.client.SearchableTextField;

import java.util.function.Function;

public class SearchableLettuceTextField<E> extends SearchableLettuceField<E> implements SearchableTextField {

    public SearchableLettuceTextField(String name,
                                      boolean sortable,
                                      Function<E, String> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, Text.builder(name).sortable(sortable).build());
    }
}
