package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GraphPerfTest {

    private static TransactionalGraph graphDb;

    @Rule
    public final Neo4jRule remoteDb = new Neo4jRule().withConfig("auth_enabled", "true");

    @Before
    public void createNeo4jConnection() throws Exception {
        // The strategy TRUST_ON_FIRST_USE doesn't work on subsequent executions,
        // reference that generated key instead.
        File dataDirectory = remoteDb.getConfig().get(DatabaseManagementSystemSettings.data_directory);
        File certFile = new File(dataDirectory, "databases/graph.db/certificates/neo4j.cert");

        Configuration config = new PropertiesConfiguration();
        config.setProperty("blueprints.graph", "com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph");
        config.setProperty("blueprints.neo4j.url", remoteDb.boltURI().toString());
        config.setProperty("blueprints.neo4j.certFile", certFile.toString());

        graphDb = (TransactionalGraph) GraphFactory.open(config);
    }

    @Test
    public void testNeo4j() {
        int MAXPAIRS = 1000;
        int MAXVERTPROPS = 10;
        int MAXEDGEPROPS = 0;

        Vertex vertex = null;
        Vertex lastVertex = null;
        List<Object> vertexKeys = new ArrayList<>(MAXPAIRS);

        try {
            // Create our vertex pairs, hook them up with an edge
            long start = System.currentTimeMillis();

            // Create verticies
            for (int i = 0; i < MAXPAIRS; ++i) {
                vertex = graphDb.addVertex(null);

                for (int j = 0; j < MAXVERTPROPS; ++j) {
                    vertex.setProperty("prop" + j, j);
                }

                if (lastVertex != null) {
                    Edge edge = graphDb.addEdge(null, lastVertex, vertex, "Edge " + i);
                    for (int j = 0; j < MAXEDGEPROPS; ++j) {
                        edge.setProperty("p" + j, j);
                    }
                }

                lastVertex = vertex;
                graphDb.commit();
                vertexKeys.add(vertex.getId());
            }

            long delta = System.currentTimeMillis() - start;
            double oneTime = (double) delta / (double) MAXPAIRS;

            System.out.println("Inserted " + MAXPAIRS + " vertex/edge pairs with " +
                    MAXVERTPROPS + " properties per vertex and " +
                    MAXEDGEPROPS + " properties per edge in " + delta + " msecs, " +
                    oneTime + " msecs per instance.");

            start = System.currentTimeMillis();
            int count = 1;
            Vertex v = graphDb.getVertex(vertexKeys.get(0));

            Iterable edges;

            while (v != null) {
                edges = v.getEdges(Direction.OUT, null);
                Iterator<Edge> iter = edges.iterator();

                if (iter.hasNext()) {
                    Edge e = (Edge) (edges.iterator().next());
                    v = e.getVertex(Direction.OUT);
                    ++count;
                } else {
                    v = null;
                }
            }

            delta = System.currentTimeMillis() - start;
            oneTime = (double) delta / (double) count;
            System.out.println("Traversed " + count + " vertex/edge pairs in " +
                    delta + " msecs, " +
                    oneTime + " msecs per traversal.");

            start = System.currentTimeMillis();
            for (Object id : vertexKeys) {
                v = graphDb.getVertex(id);
                if (!v.getId().equals(id)) {
                    System.out.println("Object id " + id + " is missing.");
                }

                graphDb.removeVertex(v);
            }

            delta = System.currentTimeMillis() - start;
            oneTime = (double) delta / (double) MAXPAIRS;

            System.out.println("Fetched and Deleted " + MAXPAIRS + " vertex/edge pairs with " +
                    MAXVERTPROPS + " properties per vertex and " +
                    MAXEDGEPROPS + " properties per edge in " + delta + " msecs, " +
                    oneTime + " msecs per instance.");

            graphDb.shutdown();
        } catch (Exception ex) {
            System.out.println("Caught exception " + ex.toString());
        } finally {
//            if (ds != null) {
//                try {
//                    JDBCUtils.doUpdateQuery(ds.getConnection(), "delete from TGP_EDGE", null);
//                    JDBCUtils.doUpdateQuery(ds.getConnection(), "delete from TGP_VERTEX", null);
//                    ds.close();
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//            }
        }
    }
}
