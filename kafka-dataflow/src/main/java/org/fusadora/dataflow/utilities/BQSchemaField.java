package org.fusadora.dataflow.utilities;

import com.google.common.base.Objects;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Utility to read bigquery schema json file and convert it to a bigquery schema
 * object.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class BQSchemaField implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private String name;
    private String mode;
    private String type;
    private String defaultValue;
    private Integer min;
    private List<BQSchemaField> fields;
    private boolean isDerived = false;
    private String derivedFrom;

    public BQSchemaField() {
        // blank constructor for AvroCoder
    }

    public BQSchemaField(String name, String type, String mode, String defaultValue) {
        this(name, type, mode, defaultValue, null);
    }

    public BQSchemaField(String name, String type, String mode, String defaultValue, List<BQSchemaField> fields) {
        this.name = name;
        this.type = type;
        this.mode = mode;
        this.defaultValue = defaultValue;
        this.fields = fields;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Integer getMin() {
        return min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }

    public List<BQSchemaField> getFields() {
        return fields;
    }

    public void setFields(List<BQSchemaField> fields) {
        this.fields = fields;
    }

    public boolean getIsDerived() {
        return isDerived;
    }

    public void setIsDerived(boolean derived) {
        isDerived = derived;
    }

    public String getDerivedFrom() {
        return derivedFrom;
    }

    public void setDerivedFrom(String derivedFrom) {
        this.derivedFrom = derivedFrom;
    }

    @Override
    @SuppressWarnings({"squid:MethodCyclomaticComplexity", "squid:S1067"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BQSchemaField that)) {
            return false;
        }
        return that.canEqual(this) && Objects.equal(name, that.name) && Objects.equal(mode, that.mode)
                && Objects.equal(type, that.type) && Objects.equal(defaultValue, that.defaultValue)
                && Objects.equal(min, that.min) && Objects.equal(fields, that.fields)
                && Objects.equal(isDerived, that.isDerived) && Objects.equal(derivedFrom, that.derivedFrom);
    }

    public boolean canEqual(Object o) {
        return o instanceof BQSchemaField;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, mode, type, defaultValue, fields, min, isDerived, derivedFrom);
    }
}
