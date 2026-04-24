package org.fusadora.dataflow.utilities;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility to read Resources folder files.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class ResourceReader {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, String> loadedResources = new HashMap<>();

    public static String getResourceOrNull(String resourceName) {
        try {
            return getResource(resourceName);
        } catch (IOException | NullPointerException e) {
            LOG.warn("Failed to get resource [{}}]", resourceName, e);
            return null;
        }
    }

    /**
     * Retrieve loaded resource and returns it
     *
     * @param resourceName resource name to be loaded
     * @return loaded resource as string
     * @throws IOException if resource cannot be loaded
     */
    public static String getResource(String resourceName) throws IOException {
        if (StringUtils.isBlank(resourceName)) {
            LOG.warn("Cannot read resource for null / blank resource name");
            return null;
        }
        String resourceToLoad = resourceName;
        if (!resourceToLoad.startsWith("/")) {
            resourceToLoad = "/" + resourceToLoad;
        }
        String loadedResource;
        if (loadedResources.containsKey(resourceToLoad)) {
            loadedResource = loadedResources.get(resourceToLoad);
            if (null != loadedResource) {
                return loadedResource;
            }
        }

        loadedResource = read(resourceToLoad);
        if (null != loadedResource) {
            loadedResources.put(resourceToLoad, loadedResource);
        }
        return loadedResource;
    }

    /**
     * Reads and returns the resource.
     *
     * @param resourceToLoad resource name to be loaded
     * @return loaded resource as string
     * @throws IOException if resource cannot be loaded
     */
    private static String read(String resourceToLoad) throws IOException {
        InputStream is = ResourceReader.class.getResourceAsStream(resourceToLoad);
        if (null == is) {
            LOG.warn("getResourceAsStream({}), returned null", resourceToLoad);
            return null;
        }
        return read(is);
    }

    /**
     * This method receives a InputStream , read the input streamline by line and
     * collect the lines in a String builder. Then return the string value.
     *
     * @param is {@link InputStream} to be read
     * @return {@link String}   representation of the input stream
     * @throws IOException if the input stream cannot be read
     */
    public static String read(InputStream is) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder resourceStr = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                resourceStr.append(line);
            }
            return resourceStr.toString();
        } catch (IOException ioe) {
            LOG.error("Unable to Read resource file");
            throw ioe;
        }
    }

    /**
     * Generic method to read the resource and return it or return null if not
     * found.
     *
     * @param resourceName resource name to be loaded
     * @param tr           TypeReference of the resource to be read
     * @return loaded resource as object of type T or null if not found
     */
    public <T> T readResourceOrNull(String resourceName, TypeReference<?> tr) {
        try {
            return readResource(resourceName, tr);
        } catch (IOException | NullPointerException e) {
            LOG.warn("Failed to read resource [{}]", resourceName, e);
            return null;
        }
    }

    /**
     * Generic method to read the resource and return it.
     *
     * @param resourceName resource name to be loaded
     * @param tr           TypeReference of the resource to be read
     * @return loaded resource as object of type T
     * @throws IOException if resource cannot be loaded
     */
    public <T> T readResource(String resourceName, TypeReference<?> tr) throws IOException {
        String resource = getResource(resourceName);
        return (T) MAPPER.readValue(resource, tr);
    }

    public <T> T readResourceOrNull(String resourceName, Class<T> target) {
        try {
            return readResource(resourceName, target);
        } catch (IOException | NullPointerException e) {
            LOG.warn("Failed to read resource [{}]", resourceName, e);
            return null;
        }
    }

    public <T> T readResource(String resourceName, Class<T> target) throws IOException {
        String resource = getResource(resourceName);
        return MAPPER.readValue(resource, target);
    }

}
