package org.fusadora.dataflow.di;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * org.fusadora.dataflow.di.CoreBusinessLogicModule
 * Guice DI Common Module
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public abstract class CoreBusinessLogicModule extends AbstractModule {

    protected <T> void bind(Class<T> target, Class<? extends T> toBind) {
        if (null != toBind && !toBind.equals(target)) {
            bind(target).to(toBind);
        }
    }

    protected <T> void bind(Class<T> target, Class<? extends T> toBind, String name) {
        if (null != toBind && !toBind.equals(target)) {
            bind(target).annotatedWith(Names.named(name)).to(toBind);
        }
    }
}
