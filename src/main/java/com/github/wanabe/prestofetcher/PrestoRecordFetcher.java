package com.github.wanabe.prestofetcher;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.wanabe.prestofetcher.PrestoStatement.Column;

class PrestoRecordFetcher extends PrestoFetcher<PrestoRecord> {
    public PrestoRecordFetcher(final PrestoSession session) {
        super(session);
    }

    public PrestoRecordFetcher(final PrestoSession session, final String uri, final long timeout, final TimeUnit unit) {
        super(session, uri, timeout, unit);
    }

    @Override
    protected void processStatement() {
        if (statement.getData() != null) {
            for (final List<Object> values : statement.getData()) {
                final PrestoRecord record = new PrestoRecord();
                final Iterator<Object> valueIterator = values.iterator();
                for (final Column column : statement.getColumns()) {
                    record.put(column.getName(), valueIterator.next());
                }
                queue.add(new PrestoQueueItem<>(record));
            }
        }
    }
}
