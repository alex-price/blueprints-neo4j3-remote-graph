package com.tinkerpop.blueprints.impls.neo4j;


import com.tinkerpop.blueprints.Element;
import org.neo4j.driver.v1.types.Entity;

public abstract class Neo4jElement<S extends Entity> implements Element {

    protected final Neo4jGraph graphDb;
    protected S rawElement;

    public Neo4jElement(final Neo4jGraph graphDb) {
        this.graphDb = graphDb;
    }

    @Override
    public Object getId() {
        return rawElement.id();
    }

}
