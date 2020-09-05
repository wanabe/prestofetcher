package com.github.wanabe.presto_api_mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

class PrestoFetcher extends Thread {
    private String uri;
    private final LinkedBlockingQueue<PrestoRecord> queue;
    private final long timeout;
    private final TimeUnit unit;
    private ArrayList<String> columns = null;

    public PrestoFetcher(final String uri, final long timeout, final TimeUnit unit) {
        this.uri = uri;
        this.timeout = timeout;
        this.unit = unit;
        this.queue = new LinkedBlockingQueue<>();
        start();
    }

    public boolean isClosed() {
        return uri == null;
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

    void fetchColumns(final JSONObject responseJson) {
        if (columns == null && responseJson.has("columns")) {
            columns = new ArrayList<>();
            try {
                for (final Object _column : responseJson.getJSONArray("columns")) {
                    final JSONObject column = (JSONObject) _column;
                    columns.add(column.getString("name"));
                }
            } catch (final JSONException e) {
                close();
            }
        }
    }

    void fetchData(final JSONObject responseJson) {
        if (responseJson.has("data")) {
            if (columns == null) {
                return;
            }
            try {
                for (final Object line : responseJson.getJSONArray("data")) {
                    final Iterator<String> columnIterator = columns.iterator();
                    final PrestoRecord record = new PrestoRecord();
                    for (final Object o : (JSONArray) line) {
                        record.put(columnIterator.next(), o);
                    }
                    queue.add(record);
                }
            } catch (final ClassCastException | JSONException e) {
                close();
            }
        }
    }

    void fetchNextUri(final JSONObject responseJson) {
        if (responseJson.has("nextUri")) {
            try {
                uri = responseJson.getString("nextUri");
                return;
            } catch(final JSONException e) {
                // through to close
            }
        }
        close();
    }

    void close() {
        queue.add(new PrestoRecord(true));
        uri = null;
    }

    @Override
    public void run() {
        final OkHttpClient client = new OkHttpClient();

        Request request;
        Response response;
        JSONObject responseJson;

        while (uri != null) {
            request = new Request.Builder().url(uri).addHeader("X-Presto-User", "presto")
                    .addHeader("X-Presto-Catalog", "mysql").addHeader("X-Presto-Schema", "test").get().build();
            try {
                response = client.newCall(request).execute();
                responseJson = new JSONObject(response.body().string());
            } catch(final IOException|JSONException e) {
                close();
                return;
            }

            fetchColumns(responseJson);
            fetchData(responseJson);
            fetchNextUri(responseJson);
        }
    }
}
