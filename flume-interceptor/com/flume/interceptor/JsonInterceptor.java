package com.example.flume;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class JsonInterceptor implements Interceptor {

    private final Gson gson = new Gson();

    @Override
    public void initialize() {
        // Initialization code
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), StandardCharsets.UTF_8);

        // Convert the event body and headers to JSON
        String json = gson.toJson(new EventModel(headers, body));
        
        event.setBody(json.getBytes(StandardCharsets.UTF_8));
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // Cleanup code
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new JsonInterceptor();
        }

        @Override
        public void configure(Context context) {
            // Configuration code
        }
    }

    // POJO class for event representation
    private static class EventModel {
        private Map<String, String> headers;
        private String body;

        public EventModel(Map<String, String> headers, String body) {
            this.headers = headers;
            this.body = body;
        }

        // Getters and setters
    }
}
