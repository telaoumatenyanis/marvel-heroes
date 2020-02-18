package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;
    private static final String TOP_HERO_KEY = "top_heroes";
    private static final String VISITED_HERO_KEY = "visited_hero";

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        StatefulRedisConnection<String, String> client = redisClient.connect();

        return client.async().zincrby(TOP_HERO_KEY, 1, statItem.toJson().toString()).thenApply(result -> {
            client.close();
            return true;
        });
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        StatefulRedisConnection<String, String> client = redisClient.connect();

        return client.async().lpush(VISITED_HERO_KEY, statItem.toJson().toString()).thenApply(result -> {
            client.async().ltrim(VISITED_HERO_KEY, 0, 4);
            return result;
        });
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        StatefulRedisConnection<String, String> client = redisClient.connect();

        return client.async().lrange(VISITED_HERO_KEY, 0, 4).thenApply(result -> result.stream().map((item) -> StatItem.fromJson(item)).collect(Collectors.toList()));
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");

        StatefulRedisConnection<String, String> client = redisClient.connect();

        return client.async().zrevrangeWithScores(TOP_HERO_KEY, 0, count - 1).thenApply(
                result -> {
                    client.close();
                    return result.stream().map(item -> new TopStatItem(StatItem.fromJson(item.getValue()), (long) item.getScore())).collect(Collectors.toList());
                }
        );
    }
}
