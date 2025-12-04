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
import java.util.Iterator;
import java.util.List;

import static org.fusadora.dataflow.utilities.BQTableFieldSchemaConstants.*;

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
        fieldDefinitions = someFieldDefinitions;
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
            bqSchemaField.setMode(BQTableFieldSchemaConstants.MODE_OPTIONAL);
            bqSchemaField.setType(BQTableFieldSchemaConstants.TYPE_STRING);
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
        return rawSchemaFieldName.replaceAll(BQTableFieldSchemaConstants.INVALID_COLUMN_NAME_REGEX, "_")
                .replaceAll(BQTableFieldSchemaConstants.CURRENCY_CODES_EURO_REGEX, BQTableFieldSchemaConstants.CURRENCY_CODE_EURO_STRING)
                .replaceAll(BQTableFieldSchemaConstants.CURRENCY_CODES_DOLLAR_REGEX, BQTableFieldSchemaConstants.CURRENCY_CODE_DOLLAR_STRING)
                .toLowerCase();
    }

    /**
     * This method returns a list of schema fields
     *
     * @return List<Field>
     */
    public static List<Field> getFields(List<BQSchemaField> fieldDefinitions) {
        List<Field> fields = new ArrayList<>();

        for (BQSchemaField fieldDefinition : fieldDefinitions) {
            StandardSQLTypeName type = StandardSQLTypeName.STRING;
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_TIMESTAMP)) {
                type = StandardSQLTypeName.TIMESTAMP;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_INTEGER)) {
                type = StandardSQLTypeName.INT64;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_NUMERIC)) {
                type = StandardSQLTypeName.NUMERIC;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_FLOAT)) {
                type = StandardSQLTypeName.FLOAT64;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_BOOLEAN)) {
                type = StandardSQLTypeName.BOOL;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_BYTES)) {
                type = StandardSQLTypeName.BYTES;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_DATE)) {
                type = StandardSQLTypeName.DATE;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_TIME)) {
                type = StandardSQLTypeName.TIME;
            }
            if (fieldDefinition.getType().equalsIgnoreCase(TYPE_DATETIME)) {
                type = StandardSQLTypeName.DATETIME;
            }

            Field field = Field.newBuilder(fieldDefinition.getName(), type)
                    .setMode(Field.Mode.valueOf(fieldDefinition.getMode()))
                    .build();
            fields.add(field);
        }
        Field field = Field.newBuilder(VERSION_FIELD_NAME, StandardSQLTypeName.INT64)
                .setMode(Field.Mode.valueOf(MODE_REQUIRED))
                .build();
        fields.add(field);
        return fields;
    }

    public static List<BQSchemaField> generateSchemaFromTableRow(TableRow tablerow) {
        List<BQSchemaField> bqSchemaFields = new ArrayList<>();

        if (tablerow != null) {
            Iterator<String> columns = tablerow.keySet().iterator();
            while (columns.hasNext()) {
                String columnName = columns.next();
                BQSchemaField bqSchemaField = new BQSchemaField();
                bqSchemaField.setMode(BQTableFieldSchemaConstants.MODE_OPTIONAL);
                bqSchemaField.setType(TYPE_STRING);
                bqSchemaField.setName(cleanSchemaFieldName(columnName));
                bqSchemaFields.add(bqSchemaField);
                columns.remove();
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
        this.fieldDefinitions = fieldDefinitions;
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
        TableSchema tableSchema = new TableSchema().setFields(getSchemaFields(fieldDefinitions));
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
        return new TableSchema().setFields(getSchemaFields(fieldDefinitions));
    }

}
