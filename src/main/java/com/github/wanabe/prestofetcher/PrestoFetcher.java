package com.github.wanabe.prestofetcher;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

abstract class PrestoFetcher<T> extends Thread {
    protected final LinkedBlockingQueue<PrestoQueueItem<T>> queue;
    protected final long timeout;
    protected final TimeUnit unit;
    private final PrestoSession session;
    protected final PrestoStatement statement;

    public PrestoFetcher(final PrestoSession session) {
        this(session, null, 0L, null);
    }

    public PrestoFetcher(final PrestoSession session, final String uri, final long timeout, final TimeUnit unit) {
        this.session = session;
        this.timeout = timeout;
        this.unit = unit;
        this.queue = new LinkedBlockingQueue<>();
        if (uri == null) {
            statement = new PrestoStatement();
        } else {
            statement = new PrestoStatement(uri);
            start();
        }
    }

    public T poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        PrestoQueueItem<T> target = null;
        if (statement.isClosed() && queue.isEmpty()) {
            return null;
        }
        if (timeout == 0) {
            target = queue.poll();
        } else {
            target = queue.poll(timeout, unit);
        }
        if (target == null) {
            return null;
        }
        return target.val;
    }

    public T poll() throws InterruptedException {
        return poll(timeout, unit);
    }

    @Override
    public void run() {
        while (!statement.isClosed()) {
            try {
                final String statementJsonString = session.get(statement.getNextUri());
                fetchStatement(statementJsonString);
            } catch (final IOException e) {
                close();
            }
        }
    }

    public String query(final String query) {
        try {
            final String statementJsonString = session.query(query);
            fetchStatement(statementJsonString);
        } catch (final IOException e) {
            close();
        }
        return statement.getNextUri();
    }

    protected void close() {
        queue.add(new PrestoQueueItem<>(null));
        statement.close();
    }

    protected abstract void processStatement();

    protected void fetchStatement(final String statementJsonString) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        statement.clear();
        mapper.readerForUpdating(statement).readValue(statementJsonString);
        processStatement();
        if (statement.getNextUri() == null) {
            close();
        }
    }
}
