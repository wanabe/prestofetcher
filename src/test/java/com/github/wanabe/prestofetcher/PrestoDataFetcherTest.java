package com.github.wanabe.prestofetcher;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class PrestoDataFetcherTest {
    @Test
    void testPoll() {
        assertDoesNotThrow(() -> {
            final PrestoSession session = new PrestoSession("presto", "mysql", "test", "http://presto:8080");

            final PrestoRecordFetcher preFetcher = new PrestoRecordFetcher(session);
            final String nextUri = preFetcher.query("SELECT * FROM users");

            final PrestoDataFetcher fetcher = new PrestoDataFetcher(session, nextUri, 10_000, TimeUnit.MILLISECONDS);

            assertEquals(Arrays.asList(
                Arrays.asList(1, "user1"),
                Arrays.asList(2, "user2")
            ),fetcher.poll());
            assertNull(fetcher.poll());
        });
    }
}
