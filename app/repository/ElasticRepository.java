package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        if (input.isEmpty()) {
            input = "*";
        }

        String query = "{\n" +
                "    \"size\": " + size + ",\n" +
                "    \"from\": " + size * (page - 1) + ",\n" +
                "    \"query\" : {\n" +
                "                \"query_string\" : {\n" +
                "                        \"fields\": [\"name^4\", \"secretIdentities^3\", \"aliases^3\", \"description^2\", \"partners^1\"],\n" +
                "                        \"query\": \"*" + input.replaceAll(" ", "*") + "*~\" }\n" +
                "            }\n" +
                "}";

        CompletionStage<PaginatedResults<SearchedHero>> result = wsClient.url(elasticConfiguration.uri + "/heroes/_search").addHeader("Content-Type", "application/json").post(
                query
        ).thenApply(response -> {
            final List<SearchedHero> heroes = new ArrayList<>();
            final JsonNode hits = response.asJson().get("hits");
            hits.get("hits").elements().forEachRemaining(e -> {
                heroes.add(SearchedHero.fromJson(e.get("_source")));
            });
            int totalSize = hits.get("total").get("value").asInt();

            return new PaginatedResults<>(totalSize, page, Math.max(1, (int) Math.ceil((double) totalSize / (double) size)), heroes);
        });

        return result;
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{     \"suggest\": {\n" +
                        "        \"heroes-suggest\" : {\n" +
                        "            \"prefix\" : \"" + input + "\", \n" +
                        "            \"completion\" : { \n" +
                        "                \"field\" : \"suggest\" \n" +
                        "            }\n" +
                        "        }\n" +
                        "    } }"))
                .thenApply(response -> {
                    final List<SearchedHero> heroes = new ArrayList<>();
                    final JsonNode hits = response.asJson().get("suggest").get("heroes-suggest").get(0).get("options");
                    hits.elements().forEachRemaining(e -> {
                        heroes.add(SearchedHero.fromJson(e.get("_source")));
                    });

                    return heroes;
                });
    }
}
