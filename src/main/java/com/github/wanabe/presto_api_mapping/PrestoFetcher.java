package com.github.wanabe.presto_api_mapping;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import com.github.wanabe.presto_api_mapping.PrestoStatement.Column;

class PrestoFetcher extends Thread {
    private final LinkedBlockingQueue<PrestoRecord> queue;
    private final long timeout;
    private final TimeUnit unit;
    private final PrestoStatement statement;

    public PrestoFetcher(final String uri, final long timeout, final TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
        this.queue = new LinkedBlockingQueue<>();
        statement = new PrestoStatement(uri);
        start();
    }

    public boolean isClosed() {
        return statement.isClosed();
    }

    public PrestoRecord poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        PrestoRecord record = null;
        if (isClosed() && queue.isEmpty()) {
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

    void close() {
        queue.add(new PrestoRecord(true));
        statement.close();
    }

    @Override
    public void run() {
        final OkHttpClient client = new OkHttpClient();

        while (!isClosed()) {
            final Request request = new Request.Builder().url(statement.getNextUri())
                    .addHeader("X-Presto-User", "presto").addHeader("X-Presto-Catalog", "mysql")
                    .addHeader("X-Presto-Schema", "test").get().build();
            try {
                final ObjectMapper mapper = new ObjectMapper();
                final Response response = client.newCall(request).execute();
                final String responseBody = response.body().string();
                statement.clear();
                mapper.readerForUpdating(statement).readValue(responseBody);
            } catch (final IOException e) {
                close();
                return;
            }
            processStatement();
        }
    }

    private void processStatement() {
        if (statement == null) {
            return;
        }
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
