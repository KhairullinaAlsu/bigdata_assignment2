#!/usr/bin/env python3
import sys
import re
from nltk import download as nltk_download
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        try:
            nltk_download('stopwords', quiet=True)
            nltk_download('punkt_tab', quiet=True)
            self.stop_words = set(stopwords.words('english'))
            self.stemmer = PorterStemmer()
            self.doc_pattern = re.compile(r'^([^\t]+)\t([^\t]+)\t(.+)$')
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

    def clean_token(self, token):
        try:
            return (token.isalnum() and 
                    token.lower() not in self.stop_words and
                    len(token) < 50)
        except:
            return False
    
    def process_document(self, line):
        try:
            line = line.strip()
            if not line:
                return
            match = self.doc_pattern.match(line)
            if not match or len(match.groups()) != 3:
                logger.warning(f"Malformed input line: {line[:100]}...")
                return
                
            doc_id, title, content = match.groups()
            doc_key = f"{doc_id}_{title.replace(' ', '-')[:100]}"
            print(f"DOCUMENT_TEXT\t{doc_key}\t{content}")
            try:
                words = word_tokenize(content)
                self.emit_document_stats(doc_key, words)
                self.emit_term_frequencies(doc_key, words)
            except Exception as e:
                logger.error(f"Tokenization failed for doc {doc_key}: {str(e)}")
                
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")

    def emit_document_stats(self, doc_id, tokens):
        try:
            print(f"DOCUMENT_LENGTH\t{doc_id}\t{len(tokens)}")
        except:
            logger.error(f"Failed to emit stats for {doc_id}")

    def emit_term_frequencies(self, doc_id, tokens):
        try:
            frequency_map = {}
            for t in tokens:
                try:
                    if self.clean_token(t):
                        term = self.stemmer.stem(t.lower())
                        frequency_map[term] = frequency_map.get(term, 0) + 1
                except:
                    continue
            
            for term, count in frequency_map.items():
                print(f"TERM_COUNT\t{term}\t{doc_id}\t{count}")
        except Exception as e:
            logger.error(f"Frequency counting failed for {doc_id}: {str(e)}")

if __name__ == "__main__":
    try:
        processor = DocumentProcessor()
        for line in sys.stdin:
            processor.process_document(line)
    except Exception as e:
        logger.critical(f"Mapper crashed: {str(e)}")
        sys.exit(1)
