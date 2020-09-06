package com.github.wanabe.presto_api_mapping;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.github.wanabe.presto_api_mapping.PrestoStatement.Column;

class PrestoFetcher extends Thread {
    private final LinkedBlockingQueue<PrestoRecord> queue;
    private final long timeout;
    private final TimeUnit unit;
    private final PrestoSession session;
    private final PrestoStatement statement;

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

    public PrestoRecord poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        PrestoRecord record = null;
        if (statement.isClosed() && queue.isEmpty()) {
            return null;
        }
        if (timeout == 0) {
            record = queue.poll();
        } else {
            record = queue.poll(timeout, unit);
        }
        if (record != null && record.isTerminator()) {
            return null;
        }
        return record;
    }

    public PrestoRecord poll() throws InterruptedException {
        return poll(timeout, unit);
    }

    @Override
    public void run() {
        while (!statement.isClosed()) {
            try {
                fetchStatement(session.get(statement.getNextUri()));
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
            return null;
        }
        return statement.getNextUri();
    }

    private void close() {
        queue.add(new PrestoRecord(true));
        statement.close();
    }

    private void fetchStatement(final String statementJsonString) throws JsonProcessingException, JsonMappingException {
        final ObjectMapper mapper = new ObjectMapper();
        statement.clear();
        mapper.readerForUpdating(statement).readValue(statementJsonString);

        if (statement.getData() != null) {
            for (final List<Object> values : statement.getData()) {
                final PrestoRecord record = new PrestoRecord();
                final Iterator<Object> valueIterator = values.iterator();
                for (final Column column : statement.getColumns()) {
                    record.put(column.getName(), valueIterator.next());
                }
                queue.add(record);
            }
        }
        if (statement.getNextUri() == null) {
            close();
        }
    }
}
