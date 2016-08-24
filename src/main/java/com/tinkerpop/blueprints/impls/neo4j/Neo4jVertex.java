package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.neo4j.iterable.EdgeIterable;
import com.tinkerpop.blueprints.impls.neo4j.iterable.VertexIterable;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);
        if (Neo4jGraph.NODE_GLOBAL_LABEL.equals(key)) {
            // Apply magic property as label
            addLabel(value.toString());
        }
        String statement = String.format("match (n) where id(n) = {id} set n.`%s` = {value} return n", key);
        Value params = Values.parameters("id", getId(), "value", Values.value(value));
        StatementResult result = graphDb.withTx().run(statement, params);
        rawElement = result.single().get(0).asNode();
    }

    @Override
    public Object removeProperty(String key) {
        Object propValue = getProperty(key);
        Value params = Values.parameters("id", getId());
        String statement = String.format("match (n) where id(n) = {id} remove n.`%s` return n", key);
        StatementResult result = graphDb.withTx().run(statement, params);
        rawElement = result.single().get(0).asNode();
        return propValue;
    }

    @Override
    public void remove() {
        graphDb.removeVertex(this);
    }

    // Non-Blueprints API methods

    public Set<String> getLabels() {
        return StreamSupport.stream(rawElement.labels().spliterator(), false).collect(Collectors.toSet());
    }

    public void addLabel(String label) {
        String statement = String.format("match (n) where id(n) = {id} set n:`%s` return n", label);
        Value params = Values.parameters("id", getId());
        StatementResult result = graphDb.withTx().run(statement, params);
        rawElement = result.single().get(0).asNode();
    }

    public void removeLabel(String label) {
        String statement = String.format("match (n) where id(n) = {id} remove n:`%s`", label);
        Value params = Values.parameters("id", getId());
        graphDb.withTx().run(statement, params);
    }

}
