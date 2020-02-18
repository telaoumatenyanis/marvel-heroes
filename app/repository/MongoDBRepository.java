package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        String query = "{id: \"" + heroId + "\"}";
        Document document = Document.parse(query);
        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        //return CompletableFuture.completedFuture(new ArrayList<>());
        // TODO
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(Document.parse("{\"$match\": {\"identity.yearAppearance\": {\"$ne\" : \"\"}}}"));
        pipeline.add(Document.parse("{\"$group\": {_id: {yearAppearance: \"$identity.yearAppearance\", universe: \"$identity.universe\"}, count: {$sum: 1}}}"));
        pipeline.add(Document.parse("{\"$group\": {_id: \"$_id\", byUniverse: {$push: {universe: \"$_id.universe\", count: \"$count\"}}}}"));
        pipeline.add(Document.parse("{\"$sort\": {\"_id.yearAppearance\": 1}}"));
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
        return CompletableFuture.completedFuture(new ArrayList<>());
        // TODO
        // List<Document> pipeline = new ArrayList<>();
        // return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
        //         .thenApply(documents -> {
        //             return documents.stream()
        //                     .map(Document::toJson)
        //                     .map(Json::parse)
        //                     .map(jsonNode -> {
        //                         return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
        //                     })
        //                     .collect(Collectors.toList());
        //         });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
        return CompletableFuture.completedFuture(new ArrayList<>());
        // TODO
        // List<Document> pipeline = new ArrayList<>();
        // return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
        //         .thenApply(documents -> {
        //             return documents.stream()
        //                     .map(Document::toJson)
        //                     .map(Json::parse)
        //                     .map(jsonNode -> {
        //                         return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
        //                     })
        //                     .collect(Collectors.toList());
        //         });
    }

}
