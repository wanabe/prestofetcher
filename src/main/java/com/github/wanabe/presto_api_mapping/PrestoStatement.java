package com.github.wanabe.presto_api_mapping;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
class PrestoStatement {
    @JsonProperty("nextUri")
    private String nextUri;
    @JsonProperty("columns")
    private List<Column> columns;
    @JsonProperty("data")
    private List<List<Object>> data;
    @JsonProperty("stats")
    private Stats stats;

    @JsonIgnore
    private boolean isClosed = false;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Column {
        @JsonProperty("name")
        private final String name;

        @JsonCreator
        private Column(@JsonProperty("name") final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Stats {
        @JsonProperty("state")
        private final String state;

        @JsonCreator
        private Stats(@JsonProperty("state") final String state) {
            this.state = state;
        }
    }

    @JsonCreator
    public PrestoStatement() {
    }

    public PrestoStatement(final String nextUri) {
        this.nextUri = nextUri;
    }

    public void clear() {
        this.nextUri = null;
        this.columns = null;
        this.data = null;
        this.stats = null;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public String getNextUri() {
        return nextUri;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<List<Object>> getData() {
        return data;
    }

    public void close() {
        isClosed = true;
    }
}
