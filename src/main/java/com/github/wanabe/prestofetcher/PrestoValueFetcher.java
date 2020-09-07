package com.github.wanabe.prestofetcher;

import java.util.List;
import java.util.concurrent.TimeUnit;

class PrestoValueFetcher<T> extends PrestoFetcher<T> {
    public PrestoValueFetcher(final PrestoSession session, final String uri, final long timeout, final TimeUnit unit) {
        super(session, uri, timeout, unit);
    }

    @Override
    protected void close() {
        queue.add(new PrestoQueueItem<>(null));
        super.close();
    }

    @Override
    protected void processStatement() {
        if (statement.getData() != null) {
            for (final List<Object> values : statement.getData()) {
                Object val = values.get(0);
                PrestoQueueItem<T> v = new PrestoQueueItem<>((T) val);
                queue.add(v);
            }
        }
    }
}
