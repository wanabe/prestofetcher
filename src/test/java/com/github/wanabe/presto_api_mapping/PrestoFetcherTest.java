package com.github.wanabe.presto_api_mapping;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class PrestoFetcherTest {
    @Test
    void testPoll() {
        assertDoesNotThrow(() -> {
            final String nextUri = query("SELECT * FROM users");

            final PrestoFetcher fetcher = new PrestoFetcher(nextUri, 10_000, TimeUnit.MILLISECONDS);

            assertEquals(new PrestoRecord() {
                private static final long serialVersionUID = 1L;
                {
                    put("id", 1);
                    put("name", "user1");
                }
            }, fetcher.poll());
            assertEquals(new PrestoRecord() {
                private static final long serialVersionUID = 1L;
                {
                    put("id", 2);
                    put("name", "user2");
                }
            }, fetcher.poll());
            assertNull(fetcher.poll());
        });
    }

    private String query(final String query) throws IOException {
        final OkHttpClient client = new OkHttpClient();

        final RequestBody body = RequestBody.create(query, MediaType.get("text/plain; charset=utf-8"));
        final Request request = new Request.Builder().url("http://presto:8080/v1/statement")
                .addHeader("X-Presto-User", "presto").addHeader("X-Presto-Catalog", "mysql")
                .addHeader("X-Presto-Schema", "test").post(body).build();

        final Response response = client.newCall(request).execute();
        final String jsonStr = response.body().string();
        final ObjectMapper mapper = new ObjectMapper();
        final PrestoStatement statement = mapper.readValue(jsonStr, PrestoStatement.class);
        return statement.getNextUri();
    }
}
