package org.fusadora.dataflow.utilities;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.CURRENCY_CODES_DOLLAR_REGEX;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.CURRENCY_CODES_EURO_REGEX;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.CURRENCY_CODE_DOLLAR_STRING;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.CURRENCY_CODE_EURO_STRING;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.INVALID_COLUMN_NAME_REGEX;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.MODE_OPTIONAL;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.MODE_REQUIRED;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_BOOLEAN;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_BYTES;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_DATE;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_DATETIME;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_FLOAT;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_INTEGER;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_NUMERIC;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_RECORD;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_STRING;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_TIME;
import static org.fusadora.dataflow.common.BQTableFieldSchemaConstants.TYPE_TIMESTAMP;

/**
 * Utility to read a bigquery schema json file and convert it to a bigquery
 * schema object.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class BQSchema implements Serializable {
    public static final String VERSION_FIELD_NAME = "version";
    private static final Logger LOG = LoggerFactory.getLogger(BQSchema.class);
    @Serial
    private static final long serialVersionUID = 1L;
    private boolean includeVersion = true;
    private List<BQSchemaField> fieldDefinitions;
    private transient TableSchema validationSchema;
    private transient TableSchema rawSchema;
    private transient List<String> fieldNames;

    public BQSchema() {
        fieldDefinitions = new ArrayList<>();
        // public constructor
    }

    public BQSchema(List<BQSchemaField> someFieldDefinitions) {
        fieldDefinitions = someFieldDefinitions == null ? new ArrayList<>() : new ArrayList<>(someFieldDefinitions);
    }

    public static BQSchema fromFile(String fileName) {
        return new BQSchema(readBqSchemaFields(fileName));
    }

    /**
     * This method is responsible for creating new table schema
     *
     * @return TableSchema
     */
    public static TableSchema createTableSchema(List<BQSchemaField> fieldDefinitions) {
        List<TableFieldSchema> fields = new ArrayList<>();
        if (null != fieldDefinitions) {
            for (BQSchemaField bqf : fieldDefinitions) {
                fields.add(new TableFieldSchema().setName(bqf.getName()).setType(bqf.getType()).setMode(bqf.getMode()));
            }
        }
        return new TableSchema().setFields(fields);
    }

    /**
     * This function fetches resource present in a bq schema file if present and
     * returns it
     *
     * @return List<BQSchemaField>
     */
    public static List<BQSchemaField> readBqSchemaFields(String schemaFileName) {
        ResourceReader resourceReader = new ResourceReader();
        List<BQSchemaField> rs = resourceReader.readResourceOrNull(schemaFileName,
                new TypeReference<List<BQSchemaField>>() {
                });
        if (null == rs) {
            LOG.warn("Failed to read resource {}", schemaFileName);
            rs = new ArrayList<>();
        }
        return rs;
    }

    /**
     * This method returns a list of schema fields
     *
     * @return List<TableFieldSchema>
     */
    private static List<TableFieldSchema> getSchemaFields(List<BQSchemaField> fieldDefinitions) {
        List<TableFieldSchema> fields = new ArrayList<>();

        for (BQSchemaField fieldDefinition : fieldDefinitions) {
            TableFieldSchema field = new TableFieldSchema().setName(fieldDefinition.getName())
                    .setType(fieldDefinition.getType()).setMode(fieldDefinition.getMode());
            if (TYPE_RECORD.equals(fieldDefinition.getType())) {
                List<TableFieldSchema> subFields = getSchemaFields(fieldDefinition.getFields());
                field.setFields(subFields);
            }
            fields.add(field);
        }

        return fields;
    }

    /**
     * This method create Bigquery schema fields from list of column with default type as STRING and mode as NULLABLE.
     *
     * @return List<BQSchemaField>
     */
    public static List<BQSchemaField> generateDefaultSchemaFields(String[] bqFieldNames) {
        List<BQSchemaField> bqSchemaFields = new ArrayList<>();
        for (String fieldName : bqFieldNames) {
            BQSchemaField bqSchemaField = new BQSchemaField();
            bqSchemaField.setMode(MODE_OPTIONAL);
            bqSchemaField.setType(TYPE_STRING);
            bqSchemaField.setName(fieldName);
            bqSchemaFields.add(bqSchemaField);
        }
        return bqSchemaFields;
    }

    /**
     * Generate Bigquery compatible field names out of source column names.
     *
     * @return {@link java.util.Arrays} of {@link String} bqFieldNames
     */
    public static String[] generateBigqueryCompatibleColumnList(String[] columnList) {
        String[] bqFieldNames = new String[columnList.length];
        for (int i = 0; i < columnList.length; i++) {
            bqFieldNames[i] = cleanSchemaFieldName(columnList[i]);
        }
        return bqFieldNames;
    }

    public static String cleanSchemaFieldName(String rawSchemaFieldName) {
        return rawSchemaFieldName.replaceAll(INVALID_COLUMN_NAME_REGEX, "_")
                .replaceAll(CURRENCY_CODES_EURO_REGEX, CURRENCY_CODE_EURO_STRING)
                .replaceAll(CURRENCY_CODES_DOLLAR_REGEX, CURRENCY_CODE_DOLLAR_STRING)
                .toLowerCase();
    }

    /**
     * This method returns a list of schema fields
     *
     * @return List<Field>
     */
    public static List<Field> getFields(List<BQSchemaField> fieldDefinitions) {
        List<Field> fields = new ArrayList<>();
        if (fieldDefinitions != null) {
            for (BQSchemaField fieldDefinition : fieldDefinitions) {
                if (fieldDefinition != null) {
                    fields.add(toField(fieldDefinition));
                }
            }
        }
        fields.add(Field.newBuilder(VERSION_FIELD_NAME, StandardSQLTypeName.INT64)
                .setMode(Field.Mode.valueOf(MODE_REQUIRED))
                .build());
        return fields;
    }

    private static Field toField(BQSchemaField fieldDefinition) {
        StandardSQLTypeName type = sqlTypeFromString(fieldDefinition.getType());
        Field.Mode mode = fieldModeFromString(fieldDefinition.getMode());
        return Field.newBuilder(fieldDefinition.getName(), type)
                .setMode(mode)
                .build();
    }

    private static StandardSQLTypeName sqlTypeFromString(String type) {
        if (type == null) {
            return StandardSQLTypeName.STRING;
        }
        return switch (type.trim().toUpperCase()) {
            case TYPE_TIMESTAMP -> StandardSQLTypeName.TIMESTAMP;
            case TYPE_INTEGER   -> StandardSQLTypeName.INT64;
            case TYPE_NUMERIC   -> StandardSQLTypeName.NUMERIC;
            case TYPE_FLOAT     -> StandardSQLTypeName.FLOAT64;
            case TYPE_BOOLEAN   -> StandardSQLTypeName.BOOL;
            case TYPE_BYTES     -> StandardSQLTypeName.BYTES;
            case TYPE_DATE      -> StandardSQLTypeName.DATE;
            case TYPE_TIME      -> StandardSQLTypeName.TIME;
            case TYPE_DATETIME  -> StandardSQLTypeName.DATETIME;
            default             -> StandardSQLTypeName.STRING;
        };
    }

    private static Field.Mode fieldModeFromString(String mode) {
        if (mode == null) {
            return Field.Mode.NULLABLE;
        }
        try {
            return Field.Mode.valueOf(mode);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Unknown field mode '{}' - defaulting to NULLABLE", mode);
            return Field.Mode.NULLABLE;
        }
    }

    public static List<BQSchemaField> generateSchemaFromTableRow(TableRow tablerow) {
        List<BQSchemaField> bqSchemaFields = new ArrayList<>();

        if (tablerow != null) {
            for (String columnName : tablerow.keySet()) {
                BQSchemaField bqSchemaField = new BQSchemaField();
                bqSchemaField.setMode(MODE_OPTIONAL);
                bqSchemaField.setType(TYPE_STRING);
                bqSchemaField.setName(cleanSchemaFieldName(columnName));
                bqSchemaFields.add(bqSchemaField);
            }
        }
        return bqSchemaFields;
    }

    /**
     * Add validations on schema if not present
     *
     * @return
     */
    public TableSchema getValidationSchema() {
        if (null == validationSchema) {
            generateValidationSchema();
        }
        return validationSchema;
    }

    private void generateValidationSchema() {
        validationSchema = createTableSchema(fieldDefinitions);
        if (includeVersion) {
            validationSchema.getFields().add(
                    new TableFieldSchema().setName(VERSION_FIELD_NAME).setType(TYPE_INTEGER).setMode(MODE_REQUIRED));
        }
    }

    public List<BQSchemaField> getFieldDefinitions() {
        if (null == fieldDefinitions) {
            fieldDefinitions = new ArrayList<>();
        }
        return fieldDefinitions;
    }

    public void setFieldDefinitions(List<BQSchemaField> fieldDefinitions) {
        this.fieldDefinitions = fieldDefinitions == null ? new ArrayList<>() : new ArrayList<>(fieldDefinitions);
    }

    public int getActualFieldCount() {
        int count = 0;
        for (BQSchemaField field : getFieldDefinitions()) {
            if (!field.getIsDerived()) {
                count++;
            }
        }
        return count;
    }

    public TableSchema getRawSchema() {
        if (null == rawSchema) {
            generateRawSchema();
        }
        return rawSchema;
    }

    private void generateRawSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        for (BQSchemaField bqf : getFieldDefinitions()) {
            fields.add(new TableFieldSchema().setName(bqf.getName()).setType(TYPE_STRING).setMode(MODE_OPTIONAL));
        }
        if (includeVersion) {
            fields.add(new TableFieldSchema().setName(VERSION_FIELD_NAME).setType(TYPE_INTEGER).setMode(MODE_REQUIRED));
        }
        rawSchema = new TableSchema().setFields(fields);
    }

    /**
     * Returns a string of fieldnames
     */
    public List<String> getFieldNames() {
        if (null == fieldNames) {
            generateFieldNames();
        }
        return fieldNames;
    }

    private void generateFieldNames() {
        fieldNames = new ArrayList<>();
        for (BQSchemaField field : getFieldDefinitions()) {
            fieldNames.add(field.getName());
        }
    }

    @Override
    @SuppressWarnings({"squid:MethodCyclomaticComplexity", "squid:S1067"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BQSchema)) {
            return false;
        }
        BQSchema that = (BQSchema) o;
        return that.canEqual(this) && Objects.equal(fieldDefinitions, that.fieldDefinitions)
                && Objects.equal(includeVersion, that.includeVersion);
    }

    public boolean canEqual(Object o) {
        return o instanceof BQSchema;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldDefinitions, includeVersion);
    }

    public BQSchema withVersions(boolean enableVersions) {
        includeVersion = enableVersions;
        return this;
    }

    /**
     * Returns a table schema
     *
     * @return
     */
    public TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema().setFields(getSchemaFields(getFieldDefinitions()));
        tableSchema.getFields()
                .add(new TableFieldSchema().setName(VERSION_FIELD_NAME).setType(TYPE_INTEGER).setMode(MODE_REQUIRED));
        return tableSchema;
    }

    /**
     * Returns a table schema without version
     *
     * @return
     */
    public TableSchema getTableSchemaWithOutVersion() {
        return new TableSchema().setFields(getSchemaFields(getFieldDefinitions()));
    }

}
