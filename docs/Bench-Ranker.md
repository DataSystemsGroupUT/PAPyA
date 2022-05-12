# Bench-Ranker

### Intro
This module is used to benchmark user's big data solutions with  a prescriptive performance analysis approach. Bench-Ranker reduces the time required to calculate the rankings, obtain useful visualizations, and determine the best performing configurations of the user. Furthermore, the key performance index of this module is extensible to anything that is measurable for a specific implementation (i.e. query runtimes). Bench-Ranker also provides an easy and interactive environment with python's Jupyter Notebook making it easy for users to get insights of their data.

To make the module scalable over the configurations space, Bench-Ranker allows plug in of any number of dimensions into the solution space for example the schemas, partitioning, and storage format. In addition, Bench-Ranker implements both Single Dimension Ranking (SD) and Multi Dimension Ranking (MD). With both solutions of Single and Multi Dimension Rankings, Bench-Ranker provides easy visualization which the user can specify themselves when interacting with the Notebook. Lastly, the system used conformance and coherence to evaluate the goodness of a ranking criteria to select which ranking criterion is "good". Meaning that the ranking does not suggest a low-performing configurations. We are looking at all ranking criteria (single dimension and multi dimension criteria) and compare them to the results accross different scales (i.e. dataset sizes). 

### Single Dimension Ranking
Bench-Ranker apply the ranking criteria for each dimension using ranking function _R_  which is the rank score of the ranked dimension (i.e. shcemas, partition, storage formats). A rank set _R_ is an ordered set of elements ordered by a score. Below is the generalized version of the ranking function which calculates the rank scores for the configurations:

<p>
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/rankingFunction.png"/>
</p>

_R_ is the rank score of the ranked dimension. Such that _d_ represents the total number of parameters (configurations) under that dimension, O _dim_ (r) denotes the number of occurences of the dimension being placed at the rank _r_ (1st, 2nd, ...), and |Q| represents the total number of queries, as we have 11 query executions in the experiment (i.e. |Q| = 11).

### Multi Dimension Ranking