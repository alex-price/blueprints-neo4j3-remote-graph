package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.driver.v1.Value;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.*;

public class GraphPerfTest {

    private static TransactionalGraph graphDb;

    @ClassRule
    public static final Neo4jRule remoteDb = new Neo4jRule().withConfig("auth_enabled", "true");

    @BeforeClass
    public static void createNeo4jConnection() throws Exception {
        Configuration config = new PropertiesConfiguration();
        config.setProperty("blueprints.graph", "com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph");
        config.setProperty("blueprints.neo4j.url", remoteDb.boltURI().toString());
        config.setProperty("blueprints.neo4j.certFile", TestUtil.defaultCertFile(remoteDb.getConfig()).toString());
        graphDb = (TransactionalGraph) GraphFactory.open(config);
    }

    @Test
    public void sanityCheck() {
        Vertex v1 = graphDb.addVertex(null);
        Assert.assertNotNull(v1.getId());

        // Add Property
        v1.setProperty("k1", "k1v1");
        Value k1v1 = v1.getProperty("k1");
        Assert.assertEquals("k1v1", k1v1.asString());

        // Update Property
        v1.setProperty("k1", "k1v2");
        Value k1v2 = v1.getProperty("k1");
        Assert.assertEquals("k1v2", k1v2.asString());

        // Property Keys
        v1.setProperty("k2", "k2v");
        v1.setProperty("k3", "k3v");
        Set actualKeys = v1.getPropertyKeys();
        Set expectedKeys = new HashSet<>(Arrays.asList("k1", "k2", "k3"));
        Assert.assertEquals(expectedKeys, actualKeys);

        // Delete Property
        Value k2v = v1.removeProperty("k2");
        Assert.assertEquals("k2v", k2v.asString());
        Assert.assertTrue(((Value) v1.getProperty("k2")).isNull());

        // Add Edge
        Vertex v2 = graphDb.addVertex(null);
        Edge e1 = v1.addEdge("CONNECTS_TO", v2);
        Assert.assertEquals(v1.getId(), e1.getVertex(Direction.IN).getId());
        Assert.assertEquals(v2.getId(), e1.getVertex(Direction.OUT).getId());
        Assert.assertEquals("CONNECTS_TO", e1.getLabel());

        // Delete Vertex
        v1.remove();
        Assert.assertNull(graphDb.getVertex(v1.getId()));
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
