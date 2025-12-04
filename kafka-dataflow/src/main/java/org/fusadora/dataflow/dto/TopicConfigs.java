package org.fusadora.dataflow.dto;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Objects;
import org.fusadora.dataflow.utilities.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * org.fusadora.dataflow.dto.TopicConfigs
 * DTO class for Topic Configurations
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "topicConfigs"
})
public class TopicConfigs extends BaseDto {
    public static final Logger LOG = LoggerFactory.getLogger(TopicConfigs.class);
    public static final String CONFIG_FILE_NAME = "topic-config.json";
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<>();
    @JsonProperty("topicConfigs")
    private List<TopicConfig> topicConfigList = null;


    public TopicConfigs() {
        super();
    }

    public static TopicConfigs fromString(String jsonDefinition) {
        try {
            return MAPPER.readValue(jsonDefinition, TopicConfigs.class);
        } catch (IOException ioe) {
            LOG.warn("Error creating [TopicConfigs] from String [{}]", jsonDefinition, ioe);
            return null;
        }
    }

    public static TopicConfigs readConfig() {
        InputStream is = TopicConfigs.class.getClassLoader().getResourceAsStream(CONFIG_FILE_NAME);
        List<String> topics = Arrays.asList(PropertyUtils.getProperty(PropertyUtils.KAFKA_TOPICS).split(","));
        List<TopicConfig> topicConfigList = new ArrayList<>();
        TopicConfigs topicConfigs = null;
        try {
            topicConfigs = MAPPER.readValue(is, TopicConfigs.class);
        } catch (IOException ioe) {
            LOG.warn("Error creating [TopicConfigs] from file [{}]", CONFIG_FILE_NAME, ioe);
            return null;
        }
        for (TopicConfig topicConfig : topicConfigs.getTopicConfigList()) {
            if (topics.contains(topicConfig.getTopicName())) {
                topicConfigList.add(topicConfig);
            }
        }
        TopicConfigs filteredTopicConfigs = new TopicConfigs();
        filteredTopicConfigs.setTopicConfigList(topicConfigList);
        return filteredTopicConfigs;
    }

    @Override
    public String toString() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException jpe) {
            LOG.warn("Error writing to string", jpe);
            return "Failed to process [" + this.getClass().getName() + "] record: " + jpe.getMessage();
        }
    }

    @JsonProperty("topicConfigs")
    public List<TopicConfig> getTopicConfigList() {
        return topicConfigList;
    }

    @JsonProperty("topicConfigs")
    public void setTopicConfigList(List<TopicConfig> topicConfigList) {
        this.topicConfigList = topicConfigList;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicConfigs that)) return false;
        return Objects.equal(topicConfigList, that.topicConfigList) && Objects.equal(additionalProperties, that.additionalProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicConfigList, additionalProperties);
    }

}
