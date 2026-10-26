"""Getting data out of the ERP's REST API (spec §4.1, §9).

Two extractors share this package so they can be read side by side:

- `naive.py`: what most teams write first. It is here to be measured, not used.
- the robust extractor (M2 step 5), which handles every mess the API has.

Both talk to the API through `client.py` and touch nothing else: no
simulator, no answer key, only what a client of the API can see. The standard
library is all they need, so they can run in a Lambda as they are.
"""
