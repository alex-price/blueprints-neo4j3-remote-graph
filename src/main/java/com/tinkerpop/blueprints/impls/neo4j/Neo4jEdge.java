package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Relationship;

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
        return rawElement.type();
    }

    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);
        String statement = String.format("match ()-[r]->() where id(r) = {id} set r.`%s` = {value}", key);
        Value params = Values.parameters("id", getId(), "value", Values.value(value));
        graphDb.withTx().run(statement, params);
        rawElement = clone(this, key, value);
    }

    @Override
    public Object removeProperty(String key) {
        Object propValue = null;
        String statement = String.format("match ()-[r]->() where id(r) = {id} with r, r.`%s` as propValue remove r.`%s` return propValue", key, key);
        Value params = Values.parameters("id", getId());
        StatementResult result = graphDb.withTx().run(statement, params);
        if (result.hasNext()) {
            propValue = result.single().get(0).asObject();
            rawElement = clone(this, key);
        }
        return propValue;
    }

    @Override
    public void remove() {
        graphDb.removeEdge(this);
    }

}
