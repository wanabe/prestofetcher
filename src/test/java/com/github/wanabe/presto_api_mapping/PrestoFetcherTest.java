package com.github.wanabe.presto_api_mapping;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

public class PrestoFetcherTest {
    @Test
    void testPoll() {
        assertDoesNotThrow(() -> {
            final PrestoSession session = new PrestoSession("presto", "mysql", "test", "http://presto:8080");

            final PrestoFetcher preFetcher = new PrestoFetcher(session);
            final String nextUri = preFetcher.query("SELECT * FROM users");

            final PrestoFetcher fetcher = new PrestoFetcher(session, nextUri, 10_000, TimeUnit.MILLISECONDS);

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
