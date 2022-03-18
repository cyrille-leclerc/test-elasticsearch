package com.cyrilleleclerc.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

/**
 * Test logging capabilities
 */
public class LogsTest {
    static RestClientTransport elasticsearchTransport;
    static ElasticsearchClient elasticsearchClient;

    @Test
    public void test_text_message() throws IOException, InterruptedException {
        String indexName = "logs_with_message_as_text";
        // INDEX EXISTENCE
        boolean indexExist = elasticsearchClient.indices().exists(builder -> builder.index(List.of(indexName))).value();
        if (!indexExist) {
            CreateIndexResponse createIndexResponse = elasticsearchClient.indices().create(index ->
                    index
                            .index(indexName)
                            .settings(sb -> sb.lifecycle(lifecycle -> lifecycle.name("7-days-default")))
                            .mappings(mappings -> mappings
                                    .properties("message", pb -> pb.text(tp -> tp))
                                    .properties("@timestamp", pb -> pb.date(dp -> dp))));
            System.out.println("CREATED: " + createIndexResponse.index() + " - " + createIndexResponse.acknowledged());
        }
        String now = DateTimeFormatter.ISO_DATE_TIME.format(OffsetDateTime.now());

        // CREATE DATA
        ObjectMapper mapper = new ObjectMapper();
        String json = String.format("""
                {
                   "@timestamp" : "%s",
                   "message": "log message %s",
                   "author": "Cyrille"
                }
                """, now, now);

        ObjectNode document = mapper.readValue(json, ObjectNode.class);

        IndexResponse indexResponse = elasticsearchClient.index(ir -> ir.index(indexName).document(document));
        System.out.println("Created document " + indexResponse.id() + " in " + indexResponse.index() + " - result " + indexResponse.result());

        // VERIFY DATA
        QueryBuilders.ids().values(List.of(indexResponse.id())).build()._toQuery();

        for (int i = 0; i < 10; i++) {
            SearchResponse<ObjectNode> searchResponse = elasticsearchClient.search(builder -> builder.index(List.of(indexName)).query(QueryBuilders.ids().values(List.of(indexResponse.id())).build()._toQuery()), ObjectNode.class);
            if (searchResponse.hits().total().value() > 0) {
                searchResponse.hits().hits().stream().map(hit -> hit.id() + " (" + hit.index() + "): " + hit.source()).forEach(s -> System.out.println(s));
                break;
            } else {
                System.out.println("Wait for document to be available for read...");
                Thread.sleep(100);
            }
        }
    }

    @Test
    public void test_message_as_object() throws IOException, InterruptedException {
        String indexName = "logs_with_message_as_object";
        // INDEX EXISTENCE
        boolean indexExist = elasticsearchClient.indices().exists(builder -> builder.index(List.of(indexName))).value();
        if (!indexExist) {
            CreateIndexResponse createIndexResponse = elasticsearchClient.indices().create(index ->
                    index.index(indexName)
                            .settings(settings -> settings.lifecycle(lifecycle -> lifecycle.name("logs")))
                            .mappings(mappings -> mappings
                                    .properties("message", message -> message.object(op -> op
                                            .properties("subMessage", subMessage -> subMessage.text(tp -> tp))
                                            .properties("availabilityZone", availabilityZone -> availabilityZone.text(tp -> tp))
                                            .properties("system", system -> system.text(tp -> tp))))
                                    .properties("@timestamp", timestamp -> timestamp.date(dp -> dp))));
            System.out.println("CREATED: " + createIndexResponse.index() + " - " + createIndexResponse.acknowledged());
        }
        // CREATE DATA
        ObjectMapper mapper = new ObjectMapper();
        String now = DateTimeFormatter.ISO_DATE_TIME.format(OffsetDateTime.now());
        String json = String.format("""
                {
                   "@timestamp" : "%s",
                   "message": {
                      "subMessage": "log sub message %s",
                      "availabilityZone": "eu-west-1",
                      "system": "my-system"
                   },
                   "author": "Cyrille"                   
                }
                """, now, now);

        ObjectNode document = mapper.readValue(json, ObjectNode.class);

        IndexResponse indexResponse = elasticsearchClient.index(ir -> ir.index(indexName).document(document));
        System.out.println("Created document " + indexResponse.id() + " in " + indexResponse.index() + " - result " + indexResponse.result());

        // VERIFY DATA
        QueryBuilders.ids().values(List.of(indexResponse.id())).build()._toQuery();

        for (int i = 0; i < 10; i++) {
            SearchResponse<ObjectNode> searchResponse = elasticsearchClient.search(builder -> builder.index(List.of(indexName)).query(QueryBuilders.ids().values(List.of(indexResponse.id())).build()._toQuery()), ObjectNode.class);
            if (searchResponse.hits().total().value() > 0) {
                searchResponse.hits().hits().stream().map(hit -> hit.id() + " (" + hit.index() + "): " + hit.source()).forEach(s -> System.out.println(s));
                break;
            } else {
                System.out.println("Wait for document to be available for read...");
                Thread.sleep(100);
            }
        }

    }


    @BeforeClass
    public static void beforeClass() throws Exception {
        InputStream envAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(".env");
        Preconditions.checkNotNull(envAsStream, ".env file not found in classpath");
        Properties env = new Properties();
        env.load(envAsStream);

        String elasticsearchUrl = env.getProperty("elasticsearch.url");
        String elasticsearchUsername = env.getProperty("elasticsearch.username");
        String elasticsearchPassword = env.getProperty("elasticsearch.password");

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticsearchUsername, elasticsearchPassword));

        RestClient restClient = RestClient.builder(HttpHost.create(elasticsearchUrl))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .build();
        elasticsearchTransport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        elasticsearchClient = new ElasticsearchClient(elasticsearchTransport);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        elasticsearchTransport.close();
    }
}
