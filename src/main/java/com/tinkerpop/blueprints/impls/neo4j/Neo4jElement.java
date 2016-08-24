package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.Element;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class Neo4jElement<S extends Entity> implements Element {

    protected final Neo4jGraph graphDb;
    protected S rawElement;

    public Neo4jElement(final Neo4jGraph graphDb) {
        this.graphDb = graphDb;
    }

    @Override
    public Object getProperty(final String key) {
        if (rawElement.containsKey(key)) {
            return rawElement.get(key).asObject();
        }
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return StreamSupport.stream(rawElement.keys().spliterator(), false).collect(Collectors.toSet());
    }

    @Override
    public Object getId() {
        return rawElement.id();
    }

    public S getRawElement() {
        return rawElement;
    }

    protected Relationship clone(Neo4jEdge edge, String key, Object value) {
        Relationship relationship = edge.getRawElement();
        Map<String, Value> properties = cloneProps(edge, key, value);
        return new InternalRelationship(relationship.id(), relationship.startNodeId(), relationship.endNodeId(), relationship.type(), properties);
    }

    protected Relationship clone(Neo4jEdge edge, String key) {
        return clone(edge, key, null);
    }

    protected Node clone(Neo4jVertex vertex, String key, Object value) {
        Node node = vertex.getRawElement();
        Collection<String> labels = StreamSupport.stream(node.labels().spliterator(), false).collect(Collectors.toList());
        Map<String, Value> properties = cloneProps(vertex, key, value);
        return new InternalNode(node.id(), labels, properties);
    }

    protected Node clone(Neo4jVertex vertex, String key) {
        return clone(vertex, key, null);
    }

    private Map<String, Value> cloneProps(Neo4jElement element, String key, Object value) {
        Map<String, Value> properties = new HashMap(element.getRawElement().asMap(v -> Values.value(v)));
        if (value == null) {
            properties.remove(key);
        } else {
            properties.put(key, Values.value(value));
        }
        return properties;
    }

}
