package com.tinkerpop.blueprints.impls.neo4j.iterable;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import org.neo4j.driver.v1.types.Relationship;

public class EdgeIterable extends ElementIterable<Edge, Relationship> {

    public EdgeIterable(Iterable<Relationship> relationships, Neo4jGraph graph) {
        super(relationships, graph, graph.getEdgeWrapper());
    }

}
