package com.tinkerpop.blueprints.impls.neo4j.iterable;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph.ElementWrapper;
import org.neo4j.driver.v1.types.Entity;

import java.util.Iterator;;

public abstract class ElementIterable<T extends Element, S extends Entity> implements CloseableIterable<T> {

    protected final Iterable<S> elements;
    protected final Neo4jGraph graphDb;
    protected final ElementWrapper<? extends T, S> elementWrapper;

    public ElementIterable(Iterable<S> elements, Neo4jGraph graphDb, ElementWrapper<? extends T, S> elementWrapper) {
        this.elements = elements;
        this.graphDb = graphDb;
        this.elementWrapper = elementWrapper;
    }

    @Override
    public void close() {
        return;
    }

    @Override
    public Iterator<T> iterator() {
        final Iterator<S> elementIterator = elements.iterator();
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return elementIterator.hasNext();
            }

            @Override
            public T next() {
                return elementWrapper.wrap(elementIterator.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
