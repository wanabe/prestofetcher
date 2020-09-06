package com.github.wanabe.presto_api_mapping;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

public class PrestoFetcherTest {
    @Test
    void testPoll() {
        assertDoesNotThrow(() -> {
            final PrestoFetcher preFetcher = new PrestoFetcher();
            final String nextUri = preFetcher.query("http://presto:8080", "SELECT * FROM users");

            final PrestoFetcher fetcher = new PrestoFetcher(nextUri, 10_000, TimeUnit.MILLISECONDS);
            fetcher.start();

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
}
