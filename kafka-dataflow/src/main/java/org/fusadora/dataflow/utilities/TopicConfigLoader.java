package org.fusadora.dataflow.utilities;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.dto.TopicConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Loads and filters {@link TopicConfigs} from the classpath configuration file.
 * Extracted from the DTO layer so DTOs remain pure data holders.
 *
 * @author Parag Ghosh
 * @since 04/2026
 */
public final class TopicConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(TopicConfigLoader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopicConfigLoader() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Reads all topic configs from the classpath file and filters to only those
     * whose topicName appears in the {@code kafka.topics} property.
     *
     * @return filtered {@link TopicConfigs}, or {@code null} if the file cannot be read
     */
    public static TopicConfigs readConfig() {
        List<String> topics = Arrays.asList(
                PropertyUtils.getProperty(PropertyUtils.KAFKA_TOPICS).split(","));

        TopicConfigs all = readAll();
        if (all == null) {
            return null;
        }

        List<TopicConfig> filtered = new ArrayList<>();
        for (TopicConfig topicConfig : all.getTopicConfigList()) {
            if (topics.contains(topicConfig.getTopicName())) {
                filtered.add(topicConfig);
            }
        }
        TopicConfigs result = new TopicConfigs();
        result.setTopicConfigList(filtered);
        return result;
    }

    /**
     * Finds a single {@link TopicConfig} by topic name from the classpath file.
     *
     * @param topicName the topic name to look up
     * @return the matching {@link TopicConfig}, or {@code null} if not found
     */
    public static TopicConfig readConfig(String topicName) {
        TopicConfigs all = readAll();
        if (all == null) {
            return null;
        }
        for (TopicConfig topicConfig : all.getTopicConfigList()) {
            if (topicName.contains(topicConfig.getTopicName())) {
                return topicConfig;
            }
        }
        LOG.error("No TopicConfig found for topic [{}]", topicName);
        return null;
    }

    private static TopicConfigs readAll() {
        InputStream is = TopicConfigLoader.class.getClassLoader()
                .getResourceAsStream(TopicConfigs.CONFIG_FILE_NAME);
        try {
            return MAPPER.readValue(is, TopicConfigs.class);
        } catch (IOException ioe) {
            LOG.warn("Error reading TopicConfigs from file [{}]", TopicConfigs.CONFIG_FILE_NAME, ioe);
            return null;
        }
    }
}

