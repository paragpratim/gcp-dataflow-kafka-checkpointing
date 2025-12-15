package org.fusadora.dataflow.utilities;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a utility to get the properties values from configuration files.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class PropertyUtils {

    // constants
    public static final String GCS_BUCKET_FILE_SEPARATOR = "/";
    public static final String GCS_URL_HEADER = "gs://";
    // properties
    // Project Config
    public static final String PROJECT_NAME = "project.name";
    public static final String PROJECT_ALIAS = "project.alias";

    //GCS
    public static final String BUCKET_DATAFLOW_STAGING = "bucket.dataflow.staging";

    //kafka
    public static final String KAFKA_NODE_HOST_1 = "kafka.node.host.1";
    public static final String KAFKA_NODE_HOST_2 = "kafka.node.host.2";
    public static final String KAFKA_NODE_HOST_3 = "kafka.node.host.3";
    public static final String KAFKA_TOPICS = "kafka.topics";
    public static final String KAFKA_CONSUMER_CLIENT_ID = "kafka.consumer.client.id";
    public static final String KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.group.id";
    public static final String KAFKA_SASL_USERNAME = "kafka.sasl.username";
    public static final String KAFKA_SASL_PASSWORD = "kafka.sasl.password";


    private static final Logger LOG = LoggerFactory.getLogger(PropertyUtils.class);
    protected static PropertyUtils instance = null;

    @Inject
    protected PropertyUtils() {
        // a private constructor for utility class
    }

    public static void initialise() {
        instance = new PropertyUtils();
    }

    public static PropertyUtils getInstance() {
        if (null == instance) {
            LOG.warn("No 'PropertyUtils' initialised - defaulting to version without caching");
            initialise();
        }
        return instance;
    }

    /**
     * Get the property for the provided key.
     *
     * @param key the key to get the property for
     * @return the property
     */
    public static String getProperty(String key) {
        return getInstance().getConfigProperty(key);
    }

    /**
     * Get the property from datastore .Datastore is configured with few of the
     * properties.This method internally calls getCachedConfigProperty
     *
     * @param key
     * @return
     */
    public String getConfigProperty(String key) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key), "Cannot get property with a null key");
        return getStaticConfigProperty(key);
    }

    /**
     * Get the properties from the configuration file.
     *
     * @param key
     * @return
     */
    public String getStaticConfigProperty(String key) {
        return Config.getProperty(key);
    }


}