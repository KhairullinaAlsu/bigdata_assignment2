#!/usr/bin/env python3

import sys
import math
import logging
from typing import Dict, List, Tuple
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, dict_factory
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import re
import nltk

nltk.download('stopwords', quiet=True)
nltk.download('punkt_tab', quiet=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

class DocumentSearchEngine:
    def __init__(self):
        self.stemmer = PorterStemmer()
        self.stopwords = set(stopwords.words('english'))
        self.spark = self._initialize_spark()
        self.cassandra_cluster = None
        self.cassandra_session = None

    def _initialize_spark(self):
        return SparkSession.builder \
            .appName("DocumentSearchEngine") \
            .config("spark.cassandra.connection.host", "cassandra-node") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

    def connect_to_cassandra(self):
        self.cassandra_cluster = Cluster(['cassandra-node'])
        self.cassandra_session = self.cassandra_cluster.connect('search_engine')
        self.cassandra_session.row_factory = dict_factory
        logger.info("Connected to Cassandra database")

    def preprocess_query(self, query_text: str) -> List[str]:
        cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', ' ', query_text.lower())
        tokens = word_tokenize(cleaned_text)
        return [
            self.stemmer.stem(token)
            for token in tokens
            if token.isalnum() and token not in self.stopwords
        ]

    def calculate_bm25_score(
        self,
        query_terms: List[str],
        doc_id: str,
        doc_length: int,
        avg_doc_length: float,
        term_frequencies: Dict[str, Dict[str, int]],
        doc_frequencies: Dict[str, int],
        total_docs: int,
        k1: float = 1.2,
        b: float = 0.75
    ) -> float:
        score = 0.0
        for term in query_terms:
            if term in term_frequencies and doc_id in term_frequencies[term]:
                term_freq = term_frequencies[term][doc_id]
                doc_freq = doc_frequencies.get(term, 0)
                
                if doc_freq > 0:
                    # Calculate IDF component
                    idf = math.log((total_docs - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0)
                    # Calculate TF component
                    tf_component = (term_freq * (k1 + 1)) / (
                        term_freq + k1 * (1 - b + b * (doc_length / avg_doc_length))
                    )
                    score += idf * tf_component
        return score

    def fetch_document_metadata(self) -> Tuple[Dict, Dict, Dict]:
        logger.info("Fetching document metadata")
        
        # Get document lengths
        doc_lengths = {
            row['doc_id']: row['doc_len']
            for row in self.cassandra_session.execute("SELECT doc_id, doc_len FROM document_lengths")
        }
        
        # Get document frequencies
        doc_frequencies = {
            row['term']: row['doc_freq']
            for row in self.cassandra_session.execute("SELECT term, doc_freq FROM document_frequencies")
        }
        
        # Get document contents
        doc_contents = {
            row['doc_id']: row['content']
            for row in self.cassandra_session.execute("SELECT doc_id, content FROM document_contents")
        }
        
        return doc_lengths, doc_frequencies, doc_contents

    def fetch_term_frequencies(self, terms: List[str]) -> Dict[str, Dict[str, int]]:
        term_frequencies = {}
        for term in terms:
            query = SimpleStatement(
                "SELECT doc_id, frequency FROM term_occurrences WHERE term = %s",
                fetch_size=1000
            )
            results = self.cassandra_session.execute(query, [term])
            term_frequencies[term] = {row['doc_id']: row['frequency'] for row in results}
        return term_frequencies

    def search(self, query_text: str, top_n: int = 5) -> Tuple[List[Tuple[str, float]], Dict[str, str]]:
        query_terms = self.preprocess_query(query_text)
        if not query_terms:
            logger.warning("Query contained no valid terms after preprocessing")
            return [], {}

        try:
            self.connect_to_cassandra()
            
            doc_lengths, doc_frequencies, doc_contents = self.fetch_document_metadata()
            term_frequencies = self.fetch_term_frequencies(query_terms)
            
            if not doc_lengths:
                logger.error("No documents found in database")
                return [], {}

            avg_doc_length = sum(doc_lengths.values()) / len(doc_lengths)
            doc_ids = list(doc_lengths.keys())
            
            # Parallelize scoring with Spark
            doc_ids_rdd = self.spark.sparkContext.parallelize(doc_ids)
            scored_docs = doc_ids_rdd.map(
                lambda doc_id: (
                    doc_id,
                    self.calculate_bm25_score(
                        query_terms,
                        doc_id,
                        doc_lengths.get(doc_id, 0),
                        avg_doc_length,
                        term_frequencies,
                        doc_frequencies,
                        len(doc_lengths)
                    )
                )
            )
            
            top_results = scored_docs.sortBy(lambda x: -x[1]).take(top_n)
            return top_results, doc_contents
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise
        finally:
            if self.cassandra_cluster:
                self.cassandra_cluster.shutdown()

    def display_results(self, query_text: str, results: List[Tuple[str, float]], contents: Dict[str, str]):
        print(f"\nTop {len(results)} results for: '{query_text}'")
        print("=" * 60)
        
        if not results:
            print("No relevant documents found")
            return
            
        for rank, (doc_id, score) in enumerate(results, 1):
            print(f"{rank}. Document ID: {doc_id}")
            print(f"   Relevance Score: {score:.4f}")
            preview = contents.get(doc_id, "Content not available")
            print(f"   Preview: {preview[:200]}...\n")

def main():
    if len(sys.argv) < 2:
        print("Usage: ./search.py <query>")
        sys.exit(1)
        
    query_text = " ".join(sys.argv[1:])
    search_engine = DocumentSearchEngine()
    
    try:
        results, contents = search_engine.search(query_text)
        search_engine.display_results(query_text, results, contents)
    except Exception as e:
        logger.critical(f"Fatal error in search: {str(e)}")
        sys.exit(1)
    finally:
        search_engine.spark.stop()

if __name__ == "__main__":
    main()
