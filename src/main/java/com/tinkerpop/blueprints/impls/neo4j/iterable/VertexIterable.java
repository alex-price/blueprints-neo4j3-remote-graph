package com.tinkerpop.blueprints.impls.neo4j.iterable;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import org.neo4j.driver.v1.types.Node;

public class VertexIterable extends ElementIterable<Vertex, Node>{

    public VertexIterable(Iterable<Node> nodes, Neo4jGraph graph) {
        super(nodes, graph, graph.getVertexWrapper());
    }

}
