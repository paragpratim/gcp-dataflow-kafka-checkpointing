package org.fusadora.dataflow.di;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * org.fusadora.dataflow.di.GuiceInitialiser
 * Guice Initialiser helper.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class GuiceInitialiser {

    private static final Logger LOG = LoggerFactory.getLogger(GuiceInitialiser.class);

    private GuiceInitialiser() {
        // a private constructor to satisfy Sonar
    }

    /**
     * Get an instance of the given class with Guice initialized.
     *
     * @param clazz the class to get an instance of
     * @param <T>   the class type
     * @return Guice initialized class
     */
    public static <T> T getGuiceInitialisedClass(AbstractModule businessModule, Class<T> clazz) {
        LOG.info("Creating Guice injector [{}] for [{}]", businessModule.getClass().getName(), clazz.getName());
        Injector injector = Guice.createInjector(businessModule);

        return injector.getInstance(clazz);
    }

    /**
     * Get a named instance of the given class with Guice initialized.
     *
     * @param clazz the class to get an instance of
     * @param <T>   the class type
     * @param name  the annotation name
     * @return Guice initialized class
     */
    public static <T> T getGuiceInitialisedClass(AbstractModule businessModule, Class<T> clazz, String name) {
        LOG.info("Creating Guice injector [{}] for [{}] with name [{}]", businessModule.getClass().getName(), clazz.getName(), name);
        Injector injector = Guice.createInjector(businessModule);

        return injector.getInstance(Key.get(clazz, Names.named(name)));
    }

}
