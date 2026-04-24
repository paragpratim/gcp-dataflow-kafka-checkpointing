package org.fusadora.dataflow.utilities;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Static utility for reading application properties from the configuration file.
 * Delegates to {@link Config} for the actual property lookup.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public final class PropertyUtils {

    // GCS helpers
    public static final String GCS_BUCKET_FILE_SEPARATOR = "/";
    public static final String GCS_URL_HEADER = "gs://";

    // Project Config
    public static final String PROJECT_NAME = "project.name";
    public static final String PROJECT_ALIAS = "project.alias";

    // GCS
    public static final String BUCKET_DATAFLOW_STAGING = "bucket.dataflow.staging";

    // Kafka
    public static final String KAFKA_BROKER_HOST = "kafka.broker.host";
    public static final String KAFKA_TOPICS = "kafka.topics";
    public static final String KAFKA_CONSUMER_CLIENT_ID = "kafka.consumer.client.id";
    public static final String KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.group.id";
    public static final String KAFKA_SASL_USERNAME = "kafka.sasl.username";
    public static final String KAFKA_SASL_PASSWORD = "kafka.sasl.password";
    // Checkpoint
    public static final String CHECKPOINT_COLLECTION = "checkpoint.collection";
    public static final String CHECKPOINT_FIRESTORE_DATABASE_ID = "checkpoint.firestore.database.id";
    public static final String CHECKPOINT_BOOTSTRAP_ENABLED = "checkpoint.bootstrap.enabled";
    public static final String CHECKPOINT_COMMIT_INTERVAL_SECONDS = "checkpoint.commit.interval.seconds";
    public static final String OFFSET_GAP_TIMEOUT_SECONDS = "offset.gap.timeout.seconds";
    public static final String OFFSET_GAP_AUDIT_ENABLED = "offset.gap.audit.enabled";
    private static final String DEFAULT_KAFKA_SECURITY_PROTOCOL = "SASL_SSL";

    private PropertyUtils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Returns the value for {@code key} from the application properties file.
     *
     * @param key the property key — must not be blank
     * @return the property value, or {@code null} if not found
     */
    public static String getProperty(String key) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key), "Cannot get property with a blank key");
        return Config.getProperty(key);
    }

    /**
     * Converts URI-style Kafka bootstrap endpoints (e.g. SASL_SSL://host:9092)
     * into Kafka bootstrap.servers format (host:9092).
     */
    public static String normalizeKafkaBootstrapServers(String bootstrapServers) {
        Preconditions.checkArgument(StringUtils.isNotBlank(bootstrapServers), "Kafka bootstrap servers cannot be blank");

        return Arrays.stream(bootstrapServers.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(PropertyUtils::stripProtocolPrefix)
                .map(PropertyUtils::stripTrailingSlash)
                .collect(Collectors.joining(","));
    }

    /**
     * Uses the URI scheme from bootstrap servers as Kafka security protocol when present.
     */
    public static String resolveKafkaSecurityProtocol(String bootstrapServers) {
        if (StringUtils.isBlank(bootstrapServers)) {
            return DEFAULT_KAFKA_SECURITY_PROTOCOL;
        }

        for (String server : bootstrapServers.split(",")) {
            String value = server.trim();
            int protocolSeparatorIndex = value.indexOf("://");
            if (protocolSeparatorIndex > 0) {
                return value.substring(0, protocolSeparatorIndex).trim().toUpperCase(Locale.ROOT);
            }
        }

        return DEFAULT_KAFKA_SECURITY_PROTOCOL;
    }

    private static String stripProtocolPrefix(String server) {
        int protocolSeparatorIndex = server.indexOf("://");
        if (protocolSeparatorIndex > 0) {
            return server.substring(protocolSeparatorIndex + 3);
        }
        return server;
    }

    private static String stripTrailingSlash(String server) {
        if (server.endsWith("/")) {
            return server.substring(0, server.length() - 1);
        }
        return server;
    }

}