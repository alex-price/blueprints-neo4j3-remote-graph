package com.tinkerpop.blueprints.impls.neo4j;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.config.Configuration;

import java.io.File;

public class TestUtil {

    /**
     * The strategy TRUST_ON_FIRST_USE stores the trusted certificate to ~/.neo4j/known_hosts, however on later
     * executions that certificate will be different and the connection will fail.
     *
     * @param config Neo4j configuration object
     * @return File Server certificate file
     */
    protected static File defaultCertFile(Configuration config) {
        File dataDirectory = config.get(DatabaseManagementSystemSettings.data_directory);
        return new File(dataDirectory, "databases/graph.db/certificates/neo4j.cert");
    }

}
