package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.client.RediSearchOptions;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.lettuce.SearchableLettuceField;
import com.rnbwarden.redisearch.client.lettuce.SearchableLettuceTagField;
import com.rnbwarden.redisearch.client.lettuce.SearchableLettuceTextField;
import com.rnbwarden.redisearch.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.entity.SearchableField;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.client.Client;
import io.redisearch.querybuilder.QueryNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.clients.jedis.exceptions.JedisDataException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.redisearch.querybuilder.QueryBuilder.intersect;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

public class JedisRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableJedisField<E>> {

    private static final Logger logger = LoggerFactory.getLogger(JedisRediSearchClient.class);
    private final Client jRediSearchClient;

    public JedisRediSearchClient(Class<E> clazz,
                                 Client jRediSearchClient,
                                 RedisSerializer<E> redisSerializer,
                                 Long defaultMaxResults) {

        super(clazz, redisSerializer, defaultMaxResults);
        this.jRediSearchClient = jRediSearchClient;
        checkAndCreateIndex();
    }

    @SuppressWarnings("unchecked")
    protected SearchableJedisField<E> createSearchableField(RediSearchFieldType type,
                                                            String name,
                                                            boolean sortable,
                                                            Function<E, String> serializationFunction) {

        if (type == RediSearchFieldType.TEXT) {
            return new SearchableJedisTextField(name, sortable, serializationFunction);
        }
        if (type == RediSearchFieldType.TAG) {
            return new SearchableJedisTagField(name, sortable, serializationFunction);
        }
        throw new IllegalArgumentException(format("field type '%s' is not supported", type));
    }

    @Override
    protected void checkAndCreateIndex() {

        try {
            jRediSearchClient.getInfo();
        } catch (JedisDataException jde) {
            this.jRediSearchClient.createIndex(createSchema(), Client.IndexOptions.defaultOptions());
        }
    }

    private Schema createSchema() {

        Schema schema = new Schema();
        getFields().stream()
                .map(SearchableJedisField::getField)
                .forEach(schema::addField);
        return schema;
    }

    @Override
    public void dropIndex() {

        jRediSearchClient.dropIndex(true);
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        String key = getQualifiedKey(entity.getPersistenceKey());
        jRediSearchClient.addDocument(key, 1, fields, false, true, null);
    }

    @Override
    public void delete(String key) {

        jRediSearchClient.deleteDocument(getQualifiedKey(key), true);
    }

    @Override
    public Optional<E> findByKey(String key) {

        return performTimedOperation("findByKey",
                () -> ofNullable(jRediSearchClient.getDocument(getQualifiedKey(key), false))
                        .map(d -> d.get(SERIALIZED_DOCUMENT))
                        .map(b -> (byte[]) b)
                        .map(redisSerializer::deserialize)
        );
    }

    @Override
    public SearchResults find(RediSearchOptions options) {

        return performTimedOperation("search", () -> search(buildQuery(options)));
    }

    private Query buildQuery(RediSearchOptions rediSearchOptions) {

        QueryNode node = intersect();
        rediSearchOptions.getQueryFields().forEach(queryField -> node.add(queryField.getName(), queryField.getQuerySyntax()));
        Query query = new Query(node.toString());

        configureQueryOptions(rediSearchOptions, query);
        return query;
    }

    private void configureQueryOptions(RediSearchOptions rediSearchOptions, Query query) {

        if (rediSearchOptions.getOffset() != null && rediSearchOptions.getLimit() != null) {
            query.limit(rediSearchOptions.getOffset().intValue(), rediSearchOptions.getLimit().intValue());
        } else {
            query.limit(0, 1000000);
        }
        if (rediSearchOptions.isNoContent()) {
            query.setNoContent();
        }
        String sortBy = rediSearchOptions.getSortBy();
        if (sortBy != null) {
            query.setSortBy(sortBy, rediSearchOptions.isSortAscending());
        }
    }

    @Override
    protected SearchResults search(String queryString, RediSearchOptions rediSearchOptions) {

        Query query = new Query(queryString);
        configureQueryOptions(rediSearchOptions, query);
        return search(query);
    }

    private SearchResults search(Query query) {

        io.redisearch.SearchResult searchResult = jRediSearchClient.search(query, false);
        logger.debug("found {} totalResults - count {}", searchResult.totalResults, searchResult.docs.stream().filter(Objects::nonNull).count());

        return new JedisSearchResults(keyPrefix, searchResult);
    }
}