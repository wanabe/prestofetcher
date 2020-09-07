package com.github.wanabe.prestofetcher;

import java.util.List;
import java.util.concurrent.TimeUnit;

class PrestoDataFetcher extends PrestoFetcher<List<List<Object>>> {
    public PrestoDataFetcher(final PrestoSession session, final String uri, final long timeout, final TimeUnit unit) {
        super(session, uri, timeout, unit);
    }

    @Override
    protected void processStatement() {
        final List<List<Object>> data = statement.getData();
        if (data != null) {
                queue.add(new PrestoQueueItem<>(data));
        }
    }
}
