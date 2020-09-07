package com.github.wanabe.prestofetcher;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

public class PrestoRecordFetcherTest {
    @Test
    void testPoll() {
        assertDoesNotThrow(() -> {
            final PrestoSession session = new PrestoSession("presto", "mysql", "test", "http://presto:8080");

            final PrestoRecordFetcher preFetcher = new PrestoRecordFetcher(session);
            final String nextUri = preFetcher.query("SELECT * FROM users");

            final PrestoRecordFetcher fetcher = new PrestoRecordFetcher(session, nextUri, 10_000, TimeUnit.MILLISECONDS);

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
