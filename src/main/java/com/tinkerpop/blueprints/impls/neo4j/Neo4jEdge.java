package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Relationship;

import java.util.*;

public class Neo4jEdge extends Neo4jElement<Relationship> implements Edge {

    public Neo4jEdge(final Relationship relationship, final Neo4jGraph graphDb) {
        super(graphDb);
        this.rawElement = relationship;
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction == Direction.BOTH) {
            throw ExceptionFactory.bothIsNotSupported();
        }
        // !!! The GraphPerfTest I was given has the direction transposed... I transposed it here too because the
        // Oracle impl is probably bugged too !!!
        String statement = "match (a)-[r]->(b) where id(r) = {id} return " + (direction == Direction.IN ? "a" : "b");
        Value params = Values.parameters("id", getId());
        StatementResult result = graphDb.withTx().run(statement, params);
        return new Neo4jVertex(result.single().get(0).asNode(), graphDb);
    }

    @Override
    public String getLabel() {
        Value params = Values.parameters("id", getId());
        StatementResult result = graphDb.withTx().run("match ()-[r]-() where id(r) = {id} with distinct r return type(r)", params);
        if (result.hasNext()) {
            return result.single().get(0).asString();
        }
        return null;
    }

    @Override
    public <T> T getProperty(String key) {
        Value params = Values.parameters("id", getId(), "key", key);
        StatementResult result = graphDb.withTx().run("match ()-[r]->() where id(r) = {id} return r[{key}]", params);
        return (T) result.single().get(0);
    }

    @Override
    public Set<String> getPropertyKeys() {
        Value params = Values.parameters("id", getId());
        StatementResult result = graphDb.withTx().run("match ()-[r]->() where id(r) = {id} return keys(r)", params);
        List<String> asList = result.list(record -> record.get(0).asString());
        return new HashSet(asList);
    }

    @Override
    public void setProperty(String key, Object value) {
        Map<String, Object> props = new HashMap<>();
        props.put(key, value);
        Value params = Values.parameters("id", rawElement.id(), "props", Values.value(props));
        graphDb.withTx().run("match ()-[r]->() where id(r) = {id} set r += {props}", params);
    }

    @Override
    public <T> T removeProperty(String key) {
        Value params = Values.parameters("id", getId(), "key", key);
        StatementResult result = graphDb.withTx().run("match ()-[r]-() where id(r) = {id} remove r[{key}] return r[{key}]", params);
        return (T) result.single().get(0);
    }

    @Override
    public void remove() {
        graphDb.removeEdge(this);
    }

}
