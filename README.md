# blueprints-neo4j3-remote-graph

Enable remote use of Neo4j 3.x using the Blueprints API.

The following properties may included in the configuration set to the constructor.

**Required:**
* blueprints.graph=com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph

**Optional (default):**
* blueprints.neo4j.url=bolt://localhost:7687
* blueprints.neo4j.username=neo4j
* blueprints.neo4j.password=neo4j

**Optional (no default, example given):**
* blueprints.neo4j.certFile=/absolute/path/to/neo4j.cert
