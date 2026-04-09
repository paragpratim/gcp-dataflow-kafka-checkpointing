package org.fusadora.dataflow.dto;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.HashMap;
import java.util.Map;

/**
 * org.fusadora.dataflow.dto.TopicConfig
 * Kafka Topic Configuration DTO — pure data holder.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"topicName", "datasetName"})
public class TopicConfig extends BaseDto {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TopicConfig.class);

    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<>();
    @JsonProperty("topicName")
    private String topicName;
    @JsonProperty("datasetName")
    private String datasetName;

    public TopicConfig() {
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

    @JsonProperty("topicName")
    public String getTopicName() { return topicName; }

    @JsonProperty("topicName")
    public void setTopicName(String topicName) { this.topicName = topicName; }

    @JsonProperty("datasetName")
    public String getDatasetName() { return datasetName; }

    @JsonProperty("datasetName")
    public void setDatasetName(String datasetName) { this.datasetName = datasetName; }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() { return this.additionalProperties; }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) { this.additionalProperties.put(name, value); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicConfig that)) return false;
        return Objects.equal(additionalProperties, that.additionalProperties)
                && Objects.equal(topicName, that.topicName)
                && Objects.equal(datasetName, that.datasetName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(additionalProperties, topicName, datasetName);
    }
}
