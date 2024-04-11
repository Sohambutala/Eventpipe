/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flinkjob;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
	public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker:29092");
        properties.setProperty("group.id", "flink-consumer");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            "clickstream",
            new SimpleStringSchema(),
            properties);

        ObjectMapper objectMapper = new ObjectMapper();

        DataStream<String> stream = env.addSource(consumer)
            .map(value -> {
                JsonNode jsonNode = objectMapper.readTree(value);
                LOG.info("Received from source: {}", value);
                return jsonNode;
            })
            .filter(jsonNode -> jsonNode.get("event_name").asText().equals("DeleteFromCart"))
            .filter(jsonNode -> jsonNode.get("channel").asText().equalsIgnoreCase("Referral"))
            .map(jsonNode -> {
                // JsonNode jsonNode = objectMapper.readTree(value);
                String userId = jsonNode.get("user_id").asText(); // Assuming JSON structure has "user_id"
                ObjectNode root = (ObjectNode) jsonNode;
                ObjectNode coupon = root.putObject("coupon");
                coupon.put("code", "DISCOUNT10");
                String result = root.toString(); // Convert modified JsonNode back to String
                LOG.info("Mapped to: {}", result);
                return result;
            });

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
            "discount",
            new SimpleStringSchema(),
            properties);

        stream.addSink(producer);

        env.execute("Flink Clickstream Processing");    
    }
}
