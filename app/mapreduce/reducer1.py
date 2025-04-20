#!/usr/bin/env python3
import sys
from collections import defaultdict

class TermFrequencyReducer:
    def __init__(self):
        self.document_lengths = {}
        self.document_texts = {}
        self.term_document_freq = defaultdict(dict)
        self.vocabulary = set()

    def process_input(self):
        # process input lines
        for line in sys.stdin:
            self.process_line(line.strip())
    
    def process_line(self, line):
        # parse and categorize each input line
        try:
            input = line.split('\t')
            if not input:
                return
            
            record_type = input[0]
            
            if record_type == "DOCUMENT_TEXT":
                self._handle_document_text(input[1:])
            elif record_type == "DOCUMENT_LENGTH":
                self._handle_document_length(input[1:])
            elif record_type == "TERM_COUNT":
                self._handle_term_frequency(input[1:])
                
        except Exception as e:
            sys.stderr.write(f"Reducer Error: {str(e)}\n")

    def _handle_document_text(self, input):
        # store document text conten
        if len(input) >= 2:
            self.document_texts[input[0]] = input[1]

    def _handle_document_length(self, input):
        # store document length metrics
        if len(input) >= 2:
            self.document_lengths[input[0]] = int(input[1])

    def _handle_term_frequency(self, input):
        # store term frequency information
        if len(input) >= 3:
            term, doc_id, freq = input[0], input[1], int(input[2])
            self.term_document_freq[term][doc_id] = freq
            self.vocabulary.add(term)

    def emit_results(self):
        # call functions to output all processed data
        self._emit_document_metadata()
        self._emit_term_statistics()
        self._emit_vocabulary()
        self._emit_document_contents()

    def _emit_document_metadata(self):
        # document lengths
        for doc_id, length in self.document_lengths.items():
            print(f"DOC_SIZE\t{doc_id}\t{length}")

    def _emit_term_statistics(self):
        # term frequencies and document frequencies
        for term, doc_counts in self.term_document_freq.items():
            for doc_id, count in doc_counts.items():
                print(f"TERM_OCCURRENCE\t{term}\t{doc_id}\t{count}")
            print(f"TERM_PRESENCE\t{term}\t{len(doc_counts)}")

    def _emit_vocabulary(self):
        # all unique terms
        for term in self.vocabulary:
            print(f"UNIQUE_TERM\t{term}")

    def _emit_document_contents(self):
        # document texts
        for doc_id, text in self.document_texts.items():
            print(f"DOCUMENT_CONTENT\t{doc_id}\t{text}")

if __name__ == "__main__":
    reducer = TermFrequencyReducer()
    reducer.process_input()
    reducer.emit_results()
