package com.github.wanabe.prestofetcher;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

public class PrestoValueFetcherTest {
    @Test
    void testValue() {
        assertDoesNotThrow(() -> {
            final PrestoSession session = new PrestoSession("presto", "mysql", "test", "http://presto:8080");

            final PrestoRecordFetcher preFetcher = new PrestoRecordFetcher(session);
            final String nextUri = preFetcher.query("SELECT * FROM users");

            final PrestoValueFetcher<Integer> fetcher = new PrestoValueFetcher<>(session, nextUri, 10_000, TimeUnit.MILLISECONDS);

            assertEquals(1, fetcher.poll());
            assertEquals(2, fetcher.poll());
            assertNull(fetcher.poll());
        });
    }
}
