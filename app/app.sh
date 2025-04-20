#!/bin/bash
# Start ssh server
service ssh restart

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz
echo "Initializing cassandra"
until cqlsh cassandra-server -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
  sleep 3
done
echo "Cassandra initialized"

# Create Cassandra schema for document search application
cat > /tmp/create_search_db.cql << 'CQL_EOF'
-- Document search application schema
CREATE KEYSPACE IF NOT EXISTS search_engine 
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'replication_factor': 1
} AND durable_writes = true;

USE search_engine;

-- Document metadata tables
CREATE TABLE IF NOT EXISTS document_lengths (
    document_id text PRIMARY KEY,
    word_count int
) WITH comment='Stores document length in words';

CREATE TABLE IF NOT EXISTS document_contents (
    document_id text PRIMARY KEY,
    content text,
    title text
) WITH comment='Stores raw document content';

-- Term frequency tables
CREATE TABLE IF NOT EXISTS vocabulary (
    term text PRIMARY KEY,
    stemmed_form text
) WITH comment='Master list of all indexed terms';

CREATE TABLE IF NOT EXISTS term_occurrences (
    term text,
    document_id text,
    frequency counter,
    PRIMARY KEY (term, document_id)
) WITH comment='Term frequencies per document';

CREATE TABLE IF NOT EXISTS document_frequencies (
    term text PRIMARY KEY,
    doc_count counter
) WITH comment='Number of documents containing each term';

CREATE TABLE IF NOT EXISTS search_results (
    query_hash text PRIMARY KEY,
    results list<text>,
    timestamp timestamp
) WITH comment='Cached search results';
CQL_EOF
# Initialize cassandra
cqlsh cassandra-server -f /tmp/create_search_db.cql
# Collect data
bash prepare_data.sh
# Run the indexer
bash index.sh 
# Run the ranker
bash search.sh "This is query"
