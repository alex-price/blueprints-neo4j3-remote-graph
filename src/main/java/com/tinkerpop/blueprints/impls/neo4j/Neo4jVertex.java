package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.neo4j.iterable.EdgeIterable;
import com.tinkerpop.blueprints.impls.neo4j.iterable.VertexIterable;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.util.HashSet;
import java.util.Set;

public class Neo4jVertex extends Neo4jElement<Node> implements Vertex {

    public Neo4jVertex(Node node, final Neo4jGraph graphDb) {
        super(graphDb);
        this.rawElement = node;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        if (labels == null) {
            labels = new String[0];
        }

        StringBuilder sb = new StringBuilder("match (n)");
        if (direction == Direction.IN) {
            sb.append("<");
        }
        sb.append("-[r]-");
        if (direction == Direction.OUT) {
            sb.append(">");
        }
        sb.append("() where id(n) = {id} ");
        if (labels != null && labels.length > 0) {
            sb.append("and type(r) in {relTypes} ");
        }
        sb.append(" return r");

        Value params = Values.parameters("id", getId(), "relTypes", Values.value(labels));

        StatementResult result = graphDb.withTx().run(sb.toString(), params);
        return new EdgeIterable(result.list(record -> record.get(0).asRelationship()), graphDb);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        if (labels == null) {
            labels = new String[0];
        }

        StringBuilder sb = new StringBuilder("match (a)");
        if (direction == Direction.IN) {
            sb.append("<");
        }
        sb.append("-[r]-");
        if (direction == Direction.OUT) {
            sb.append(">");
        }
        sb.append("(b) where id(a) = {id} and type(r) in {relTypes} return b");

        Value params = Values.parameters("id", getId(), "relTypes", labels);

        StatementResult result = graphDb.withTx().run(sb.toString(), params);
        return new VertexIterable(result.list(record -> record.get(0).asNode()), graphDb);
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return graphDb.addEdge(null, this, inVertex, label);
    }

    @Override
    public <T> T getProperty(String key) {
        String statement = String.format("match (n) where id(n) = {id} return n.`%s`", key);
        Value params = Values.parameters("id", getId());
        StatementResult result = graphDb.withTx().run(statement, params);
        if (result.hasNext()) {
            return (T) result.single().get(0);
        }
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        Value params = Values.parameters("id", getId());
        StatementResult result = graphDb.withTx().run("match (n) where id(n) = {id} return keys(n)", params);
        if (result.hasNext()) {
            return new HashSet(result.single().get(0).asList());
        }
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {
        String statement = String.format("match (n) where id(n) = {id} set n.`%s` = {value}", key);
        Value params = Values.parameters("id", getId(), "value", value);
        graphDb.withTx().run(statement, params);
    }

    @Override
    public <T> T removeProperty(String key) {
        Value params = Values.parameters("id", getId());
        String statement = String.format("match (n) where id(n) = {id} with n, n.`%s` as propValue remove n.`%s` return propValue", key, key);
        StatementResult result = graphDb.withTx().run(statement, params);
        if (result.hasNext()) {
            return (T) result.single().get(0);
        }
        return null;
    }

    @Override
    public void remove() {
        graphDb.removeVertex(this);
    }

}
