package org.fusadora.dataflow.dto;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * org.fusadora.dataflow.dto.TopicConfigs
 * DTO class for Topic Configurations — pure data holder.
 * Use {@link org.fusadora.dataflow.utilities.TopicConfigLoader} to load from the classpath.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"topicConfigs"})
public class TopicConfigs extends BaseDto {

    public static final String CONFIG_FILE_NAME = "topic-config.json";
    @Serial
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TopicConfigs.class);
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<>();
    @JsonProperty("topicConfigs")
    private List<TopicConfig> topicConfigList = null;

    public TopicConfigs() {
        super();
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
        return Objects.equal(topicConfigList, that.topicConfigList)
                && Objects.equal(additionalProperties, that.additionalProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicConfigList, additionalProperties);
    }
}
