package org.fusadora.dataflow.utilities;


import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.fusadora.dataflow.exception.StaticUtilityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

/**
 * Common utilities to read and parse Properties configuration file using Apache Commons configuration library.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class Config {

    private static final Logger log = LoggerFactory.getLogger(Config.class);

    private static final String DEFAULT_CONFIG_FILE = "config.properties";
    private static final char LIST_DELIMITER = ';';
    private static final PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
    private static volatile Config singleton;

    /**
     * private constructor to reinforce the singleton pattern
     *
     */
    private Config() throws ConfigurationException {
        super();
        this.loadPropertiesFromFile();
    }

    /**
     * returns the singleton
     *
     * @return Singleton instance of Config
     */
    protected static Config getInstance() {

        if (singleton == null) {
            synchronized (propertiesConfiguration) {
                // check if still null in case another thread is still waiting
                if (singleton == null) {

                    try {
                        singleton = new Config();
                    } catch (ConfigurationException e) {
                        throw new StaticUtilityException("Failed to initialize Config instance", e);
                    }
                }
            }
            log.info("Successfully created Config Singleton");
        }

        return singleton;
    }

    /**
     * <p>
     * Return a property for the provided key
     * </p>
     *
     * @param key String key to look up in the properties file
     * @return Value for the provided key, or null if the key is not found
     */
    public static String getProperty(String key) {

        return getInstance().getConfig().getString(key);
    }

    /**
     * <p>
     * Return a property for the provided key
     * </p>
     *
     * @param key          String key to look up in the properties file
     * @param defaultValue Value to return if the key is not found in the properties file
     * @return Value for the provided key, or defaultValue if the key is not found
     */
    public static String getProperty(String key, String defaultValue) {

        return getInstance().getConfig().getString(key, defaultValue);
    }

    /**
     * <p>
     * Return a property for the provided key
     * </p>
     *
     * @param key          String key to look up in the properties file
     * @param defaultValue Value to return if the key is not found in the properties file
     * @return Value for the provided key, or defaultValue if the key is not found
     */
    public static Integer getIntegerProperty(String key, Integer defaultValue) {

        return getInstance().getConfig().getInteger(key, defaultValue);
    }

    /**
     * <p>
     * Return a property for the provided key
     * </p>
     *
     * @param key          String key to look up in the properties file
     * @param defaultValue Value to return if the key is not found in the properties file
     * @return Value for the provided key, or defaultValue if the key is not found
     */
    public static Double getDoubleProperty(String key, Double defaultValue) {

        return getInstance().getConfig().getDouble(key, defaultValue);
    }

    /**
     * <p>
     * Return a property for the provided key
     * </p>
     *
     * @param key          String key to look up in the properties file
     * @param defaultValue Value to return if the key is not found in the properties file
     * @return Value for the provided key, or defaultValue if the key is not found
     */
    public static Long getLongProperty(String key, Long defaultValue) {

        return getInstance().getConfig().getLong(key, defaultValue);
    }

    /**
     * @param key          String key to look up in the properties file
     * @param defaultValue Value to return if the key is not found in the properties file
     * @return Value for the provided key, or defaultValue if the key is not found
     */
    public static Boolean getBooleanProperty(String key, Boolean defaultValue) {

        return getInstance().getConfig().getBoolean(key, defaultValue);
    }

    /**
     * Get a list from the properties file<br>
     * properties like this<br>
     * key = This property, has multiple, values <br>
     * will be put in a list
     *
     * @param key String key to look up in the properties file
     * @return List of values for the provided key, or null if the key is not found
     */
    public static List<Object> getList(String key) {
        return getInstance().getConfig().getList(key);
    }

    /**
     * <p>
     * Get a Property object from a file
     * </p>
     *
     * @throws ConfigurationException if there is an error loading the properties file
     */
    private void loadPropertiesFromFile() throws ConfigurationException {

        URL url = Thread.currentThread().getContextClassLoader().getResource(Config.DEFAULT_CONFIG_FILE);

        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
                new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                        .configure(params.properties()
                                .setListDelimiterHandler(new DefaultListDelimiterHandler(LIST_DELIMITER))
                                .setURL(url)
                        );

        // this will load the file and return the populated configuration
        propertiesConfiguration.copy(builder.getConfiguration());

    }

    /**
     * @return the properties
     */
    protected PropertiesConfiguration getConfig() {
        return propertiesConfiguration;
    }

}
