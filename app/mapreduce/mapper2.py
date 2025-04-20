#! /usr/bin/env python3
import sys

for input in sys.stdin:
    if not input.strip():
        continue
    print(input.strip())
