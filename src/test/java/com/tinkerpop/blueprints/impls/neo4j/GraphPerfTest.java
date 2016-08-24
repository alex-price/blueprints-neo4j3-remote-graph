package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.*;

public class GraphPerfTest {

    private static Neo4jGraph graphDb;

    @ClassRule
    public static final Neo4jRule remoteDb = new Neo4jRule().withConfig("auth_enabled", "true");

    @BeforeClass
    public static void createNeo4jConnection() throws Exception {
        Configuration config = new PropertiesConfiguration();
        config.setProperty("blueprints.graph", "com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph");
        config.setProperty("blueprints.neo4j.url", remoteDb.boltURI().toString());
        config.setProperty("blueprints.neo4j.certFile", TestUtil.defaultCertFile(remoteDb.getConfig()).toString());
        graphDb = (Neo4jGraph) GraphFactory.open(config);
    }


    @Test
    public void labelTest() {
        Vertex v1 = graphDb.addVertex(null);
        v1.setProperty(Neo4jGraph.NODE_GLOBAL_LABEL, "Red");
        v1.setProperty(Neo4jGraph.NODE_GLOBAL_LABEL, "Green");
        v1.setProperty(Neo4jGraph.NODE_GLOBAL_LABEL, "Blue");
        Assert.assertTrue(((Neo4jVertex) v1).getLabels().containsAll(Arrays.asList("Red", "Green", "Blue")));
    }

    @Test
    public void queryTest() {
        try {
            graphDb.createKeyIndex("k1", Neo4jVertex.class);

            Vertex v1 = graphDb.addVertex(null);
            v1.setProperty("k1", "k1v1");
            Vertex v2 = graphDb.addVertex(null);
            v2.setProperty("k1", "k1v2");
            Vertex v3 = graphDb.addVertex(null);
            v3.setProperty("k1", "k1v1");
            graphDb.commit();

            Query query = graphDb.query().has("k1", "k1v1");
            int count = 0;

            for (Vertex v : query.vertices()) {
                Assert.assertEquals("k1v1", v.getProperty("k1"));
                count++;
            }

            Assert.assertEquals(count, 2);

            graphDb.commit();
        } catch (Exception ex) {
            graphDb.rollback();
        }
    }

    @Test
    public void sanityCheck() {
        try {
            // Vertex Add
            Vertex v1 = graphDb.addVertex(null);
            Assert.assertNotNull(v1.getId());

            // Vertex Add Property
            v1.setProperty("k1", "k1v1");
            Assert.assertEquals("k1v1", v1.getProperty("k1"));

            // Vertex Update Property
            v1.setProperty("k1", "k1v2");
            Assert.assertEquals("k1v2", v1.getProperty("k1"));

            // Vertex List Properties
            v1.setProperty("k2", "k2v");
            v1.setProperty("k3", "k3v");
            Set actualKeys = v1.getPropertyKeys();
            Set expectedKeys = new HashSet<>(Arrays.asList("k1", "k2", "k3"));
            Assert.assertEquals(expectedKeys, actualKeys);

            // Vertex Remove Property
            String k2v = v1.removeProperty("k2");
            Assert.assertEquals("k2v", k2v);
            Assert.assertNull(v1.getProperty("k2"));

            // Edge Add
            Vertex v2 = graphDb.addVertex(null);
            Edge e1 = v1.addEdge("CONNECTS_TO", v2);
            Assert.assertEquals(v1.getId(), e1.getVertex(Direction.IN).getId());
            Assert.assertEquals(v2.getId(), e1.getVertex(Direction.OUT).getId());
            Assert.assertEquals("CONNECTS_TO", e1.getLabel());

            // Edge Add/Update Property
            e1.setProperty("k1", "k1v1");
            Assert.assertEquals("k1v1", e1.getProperty("k1"));
            e1.setProperty("k1", "k1v2");
            Assert.assertEquals("k1v2", e1.getProperty("k1"));

            // Edge Remove Property
            Assert.assertEquals("k1v2", e1.removeProperty("k1"));
            Assert.assertNull(e1.getProperty("k1"));

            // Edge Remove
            e1.remove();
            Assert.assertNull(graphDb.getEdge(e1.getId()));

            // Vertex Remove
            v1.remove();
            Assert.assertNull(graphDb.getVertex(v1.getId()));

            graphDb.commit();
        } catch (Exception ex) {
            graphDb.rollback();
        }
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
