package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.neo4j.iterable.EdgeIterable;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Function;

import java.util.HashMap;
import java.util.Map;
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
            sb.append("<-[r]-");
        } else if (direction == Direction.OUT) {
            sb.append("-[r]->");
        } else {
            sb.append("-[r]-");
        }

        sb.append("() where id(n) = {id} ");

        if (labels != null && labels.length > 0) {
            sb.append("and type(r) in {relTypes} ");
        }

        sb.append(" return r");

        String statement = sb.toString();
        Value params = Values.parameters("id", getId(), "relTypes", Values.value(labels));

        StatementResult result = graphDb.defaultTx().run(statement, params);

        return new EdgeIterable(result.list(AS_NEO4J_RELATIONSHIP), graphDb);
    }

    private final Function<Record, Relationship> AS_NEO4J_RELATIONSHIP = new Function<Record, Relationship>()
    {
        public Relationship apply( Record record )
        {
            return record.get(0).asRelationship();
        }
    };

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VertexQuery query() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getProperty(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getPropertyKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty(String key, Object value) {
        Map<String, Object> props = new HashMap<>();
        props.put(key, value);
        Value params = Values.parameters("id", rawElement.id(), "props", Values.value(props));
        graphDb.defaultTx().run("match (n) where id(n) = {id} set n += {props}", params);
    }

    @Override
    public <T> T removeProperty(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
