package com.tinkerpop.blueprints.impls.neo4j;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.neo4j.iterable.EdgeIterable;
import com.tinkerpop.blueprints.impls.neo4j.iterable.VertexIterable;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.apache.commons.configuration.Configuration;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.io.File;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class Neo4jGraph implements MetaGraph<Session>, TransactionalGraph {

    private static final Logger logger = Logger.getLogger(Neo4jGraph.class.getName());

    private static Features FEATURES = new Features();

    static {
        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = true;
        FEATURES.supportsIndices = false;
        FEATURES.supportsKeyIndices = false;
        FEATURES.supportsVertexKeyIndex = false;
        FEATURES.supportsEdgeKeyIndex = false;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
    }

    public interface ElementWrapper<T extends Element, S extends Entity> {
        T wrap(S rawElement);
    }

    public interface VertexWrapper<V extends Vertex> extends ElementWrapper<V, Node>{
    }

    public interface EdgeWrapper<E extends Edge> extends ElementWrapper<E, Relationship>{
    }

    private static VertexWrapper<Neo4jVertex> createDefaultVertexWrapper(final Neo4jGraph graph){
        return rawVertex -> new Neo4jVertex(rawVertex, graph);
    }

    private static EdgeWrapper<Neo4jEdge> createDefaultEdgeWrapper(final Neo4jGraph graph){
        return rawEdge -> new Neo4jEdge(rawEdge, graph);
    }

    protected Configuration config;
    protected final Driver driver;
    protected ThreadLocal<Session> session;
    protected ThreadLocal<Transaction> tx = new ThreadLocal();

    private VertexWrapper<? extends Vertex> vertexWrapper;
    private EdgeWrapper<? extends Edge> edgeWrapper;

    public Transaction withTx() {
        if (tx.get() == null) {
            tx.set(session.get().beginTransaction());
        }
        return tx.get();
    }

    public Neo4jGraph(final Configuration argConfig) {
        this.config = argConfig.subset("blueprints.neo4j");

        Config.ConfigBuilder neo4jConfig = Config.build();

        if (config.containsKey("certFile")) {
            try {
                File certFile = Paths.get(config.getString("certFile")).toFile();
                neo4jConfig.withTrustStrategy(Config.TrustStrategy.trustSignedBy(certFile));
            } catch (Exception ex) {
                logger.warning("Unable to locate certFile");
            }
        }

        String url = config.getString("url", "bolt://localhost:7687");

        String username = config.getString("username", "neo4j");
        String password = config.getString("password", "neo4j");
        AuthToken authToken = AuthTokens.basic(username, password);

        driver = GraphDatabase.driver(url, authToken, neo4jConfig.toConfig());
        session = ThreadLocal.withInitial(driver::session);

        vertexWrapper = createDefaultVertexWrapper(this);
        edgeWrapper = createDefaultEdgeWrapper(this);
    }

    public VertexWrapper<? extends Vertex> getVertexWrapper() {
        return vertexWrapper;
    }

    public EdgeWrapper<? extends Edge> getEdgeWrapper() {
        return edgeWrapper;
    }

    public void setVertexWrapper(VertexWrapper<? extends Vertex> vertexWrapper) {
        this.vertexWrapper = vertexWrapper;
    }

    public void setEdgeWrapper(EdgeWrapper<? extends Edge> edgeWrapper) {
        this.edgeWrapper = edgeWrapper;
    }

    @Override
    public Session getRawGraph() {
        return session.get();
    }

    // TransactionalGraph

    @Override
    public void stopTransaction(Conclusion conclusion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() {
        if (tx.get() == null) {
            return;
        }

        try {
            tx.get().success();
        } finally {
            tx.get().close();
            tx.remove();
        }
    }

    @Override
    public void rollback() {
        if (tx.get() == null) {
            return;
        }

        try {
            tx.get().failure();
        } finally {
            tx.get().close();
            tx.remove();
        }
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
        StatementResult result = withTx().run("create (n) return n");
        Node node = result.single().get(0).asNode();
        return new Neo4jVertex(node, this);
    }

    @Override
    public Vertex getVertex(Object id) {
        if (null == id) {
            throw ExceptionFactory.vertexIdCanNotBeNull();
        }
        StatementResult result = withTx().run("match (n) where id(n) = {id} return n", Values.parameters("id", id));
        if (result.hasNext()) {
            Node node = result.single().get(0).asNode();
            return new Neo4jVertex(node, this);
        }
        return null;
    }

    @Override
    public void removeVertex(Vertex vertex) {
        withTx().run("match (n) where id(n) = {id} detach delete n", Values.parameters("id", vertex.getId()));
    }

    @Override
    public Iterable<Vertex> getVertices() {
        StatementResult result = withTx().run("match (n) return n");
        return new VertexIterable(result.list(record -> record.get(0).asNode()), this);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        Value params = Values.parameters("key", key, "value", value);
        StatementResult result = withTx().run("match (n) where n[{key}] = {value}", params);
        return new VertexIterable(result.list(record -> record.get(0).asNode()), this);
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (label == null) {
            throw ExceptionFactory.edgeLabelCanNotBeNull();
        }
        String statement = String.format("match (a), (b) where id(a) = {ida} and id(b) = {idb} create (a)-[r:`%s`]->(b) return r", label);
        Value params = Values.parameters("ida", outVertex.getId(), "idb", inVertex.getId());
        StatementResult result = withTx().run(statement, params);
        Relationship rel = result.single().get(0).asRelationship();
        return new Neo4jEdge(rel, this);
    }

    @Override
    public Edge getEdge(Object id) {
        StatementResult result = withTx().run("match ()-[r]-() where id(r) = {id} return r", Values.parameters("id", id));
        if (result.hasNext()) {
            result.single().get(0).asRelationship();
        }
        return null;
    }

    @Override
    public void removeEdge(Edge edge) {
        withTx().run("match ()-[r]-() where id(r) = {id} delete r", Values.parameters("id", edge.getId()));
    }

    @Override
    public Iterable<Edge> getEdges() {
        StatementResult result = withTx().run("match ()-[r]-() return r");
        return new EdgeIterable(result.list(record -> record.get(0).asRelationship()), this);
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        Value params = Values.parameters("key", key, "value", value);
        StatementResult result = withTx().run("match ()-[r]-() where r[{key}] = {value} return r", params);
        return new EdgeIterable(result.list(record -> record.get(0).asRelationship()), this);
    }

    @Override
    public GraphQuery query() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        Transaction openTx = tx.get();
        if (openTx != null) {
            openTx.success();
            openTx.close();

        }
        session.get().close();
        driver.close();
    }

}
