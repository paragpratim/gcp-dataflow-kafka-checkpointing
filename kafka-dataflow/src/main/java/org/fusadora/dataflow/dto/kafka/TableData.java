package org.fusadora.dataflow.dto.kafka;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Objects;
import org.fusadora.dataflow.dto.BaseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.util.HashMap;
import java.util.Map;

/**
 * org.fusadora.dataflow.dto.kafka.TableData
 * A DTO to hold table data with dynamic properties.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableData extends BaseDto {

    @Serial
    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(TableData.class);

    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<>();

    public TableData() {
        super();
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
    public String toString() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException jpe) {
            LOG.warn("Error writing to string", jpe);
            return "Failed to process [" + this.getClass().getName() + "] record: " + jpe.getMessage();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableData tableData)) return false;
        return Objects.equal(additionalProperties, tableData.additionalProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(additionalProperties);
    }

}
