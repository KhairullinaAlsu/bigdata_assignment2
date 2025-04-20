#!/usr/bin/env python3

import sys
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.policies import RetryPolicy
from cassandra import ConsistencyLevel
import logging

class CassandraDataLoader:
    def __init__(self):
        self.logger = self._setup_logging()
        self.cluster = None
        self.session = None
        self.batch_size = 50
        self.prepared_statements = {}
        
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            stream=sys.stderr
        )
        return logging.getLogger(__name__)
    
    def connect_to_cassandra(self):
        retry_policy = RetryPolicy()
        self.cluster = Cluster(
            ['cassandra-server'],
            connect_timeout=30,
            default_retry_policy=retry_policy
        )
        
        try:
            self.session = self.cluster.connect()
            self.session.execute("USE search_engine")
            self._prepare_statements()
            self.logger.info("Successfully connected to Cassandra")
        except Exception as e:
            self.logger.error(f"Connection failed: {str(e)}")
            raise

    def _prepare_statements(self):
        # Prepare all CQL statements for batch loading
        self.prepared_statements = {
            'vocabulary': self.session.prepare(
                "INSERT INTO vocabulary (term) VALUES (?)"
            ),
            'document_length': self.session.prepare(
                "INSERT INTO document_lengths (document_id, word_count) VALUES (?, ?)"
            ),
            'document_content': self.session.prepare(
                "INSERT INTO document_contents (document_id, content) VALUES (?, ?)"
            ),
            'term_frequency': self.session.prepare(
                "INSERT INTO term_occurrences (term, document_id, frequency) VALUES (?, ?, ?)"
            ),
            'document_frequency': self.session.prepare(
                "INSERT INTO document_frequencies (term, doc_count) VALUES (?, ?)"
            )
        }

    def process_input(self):
        # Process stdin and load data to Cassandra
        current_batch = self._new_batch()
        processed_count = 0

        for line_num, line in enumerate(sys.stdin, 1):
            try:
                line = line.strip()
                if not line:
                    continue

                parts = line.split('\t')
                record_type = parts[0]

                if record_type == "UNIQUE_TERM":
                    current_batch.add(self.prepared_statements['vocabulary'], (parts[1],))
                elif record_type == "DOC_SIZE":
                    current_batch.add(self.prepared_statements['document_length'], 
                                    (parts[1], int(parts[2])))
                elif record_type == "DOCUMENT_CONTENT":
                    current_batch.add(self.prepared_statements['document_content'], 
                                    (parts[1], parts[2]))
                elif record_type == "TERM_PRESENCE":
                    current_batch.add(self.prepared_statements['document_frequency'], 
                                    (parts[1], int(parts[2])))
                elif record_type == "TERM_OCCURRENCE":
                    current_batch.add(self.prepared_statements['term_frequency'], 
                                    (parts[1], parts[2], int(parts[3])))
                else:
                    self.logger.warning(f"Unknown record type at line {line_num}: {record_type}")
                    continue

                processed_count += 1
                if processed_count % self.batch_size == 0:
                    self._execute_batch(current_batch)
                    current_batch = self._new_batch()

            except Exception as e:
                self.logger.error(f"Error processing line {line_num}: {str(e)}")

        # Flush remaining items in batch
        if current_batch.size > 0:
            self._execute_batch(current_batch)

        self.logger.info(f"Successfully processed {processed_count} records")

    def _new_batch(self):
        # Create a new batch
        return BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)

    def _execute_batch(self, batch):
        # Execute batch with error handling
        try:
            self.session.execute(batch)
        except Exception as e:
            self.logger.error(f"Batch execution failed: {str(e)}")
            raise

    def shutdown(self):
        if self.cluster:
            self.cluster.shutdown()
            self.logger.info("Cassandra connection closed")

def main():
    loader = CassandraDataLoader()
    try:
        loader.connect_to_cassandra()
        loader.process_input()
    except Exception as e:
        loader.logger.critical(f"Fatal error: {str(e)}")
        sys.exit(1)
    finally:
        loader.shutdown()

if __name__ == "__main__":
    main()
