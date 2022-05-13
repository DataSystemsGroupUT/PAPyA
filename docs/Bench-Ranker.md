# Bench-Ranker

### Intro
This module is used to benchmark user's big data solutions with  a prescriptive performance analysis approach. Bench-Ranker reduces the time required to calculate the rankings, obtain useful visualizations, and determine the best performing configurations of the user. Furthermore, the key performance index of this module is extensible to anything that is measurable for a specific implementation (i.e. query runtimes). Bench-Ranker also provides an easy and interactive environment with python's Jupyter Notebook making it easy for users to get insights of their data.

To make the module scalable over the configurations space, Bench-Ranker allows plug in of any number of dimensions into the solution space for example the schemas, partitioning, and storage format. In addition, Bench-Ranker implements both Single Dimension Ranking (SD) and Multi Dimension Ranking (MD). With both solutions of Single and Multi Dimension Rankings, Bench-Ranker provides easy visualization which the user can specify themselves when interacting with the Notebook. Lastly, the system used conformance and coherence to evaluate the goodness of a ranking criteria to select which ranking criterion is "good". Meaning that the ranking does not suggest a low-performing configurations. We are looking at all ranking criteria (single dimension and multi dimension criteria) and compare them to the results accross different scales (i.e. dataset sizes). 

### Single Dimension Ranking
Bench-Ranker apply the ranking criteria for each dimension using ranking function _R_  which is the rank score of the ranked dimension (i.e. shcemas, partition, storage formats). A rank set _R_ is an ordered set of elements ordered by a score. Below is the generalized version of the ranking function which calculates the rank scores for the configurations:

<p>
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/rankingFunction.png"/>
</p>

_R_ is the rank score of the ranked dimension. Such that _d_ represents the total number of parameters (configurations) under that dimension, O _dim_ (r) denotes the number of occurences of the dimension being placed at the rank _r_ (1st, 2nd, ...), and Q represents the total number of queries, as we have 11 query executions in the experiment (i.e. Q = 11).

### Multi Dimension Ranking
Single dimensional ranking optimizes the configurations towards a single particular dimension ignoring the trade-offs to other dimensions. This shows that Single Dimension ranking criteria maximizes the scores only for one dimension while ignoring the others. This leads to the idea of a Multi Dimensional Ranking criteria which aims to optimize all dimensions at the same time. Bench-Ranker utilizes the Non-Dominated Sorting Genetic Algorithm 2 (NSGA-II) to find the best performing configuration solution in a complex solution space.<br>

Bench-Ranker provides two ways to apply NSGA-II algorithm:
- __ParetoQ__ <br>
: apllies the NSGA-II algorithm by considering the rank sets obtained while sorting each query results individually. Using this method, the algorithm aims to minimize the query runtimes accross all dimensions
- __ParetoAgg__ <br>
: applies NSGA-II algorithm by operating on the single dimension ranking criteria. This method aims to maximize the rank scores of the single dimensions ranking criteria altogether

### Ranking Validation  
Bench-Ranker provides a ranking solution validation for all ranking criteria (i.e. SD Ranking and MD Ranking) using the _conformance_ and _coherence_. We identify if a ranking criteria is "good" if it's not suggesting any low-performing configurations in our experiment. We are using such metric to look at all ranking criteria and comparing them on different scales (i.e. dataset sizes).<br>

- __Conformance__ <br>
: Ranking conformance is a measure of adherence to the top ranked configurations according to the query positions of the configurations<br>
<p>
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/conformanceFormula.png"/>
</p>
We calculate conformance using this equation by positioning the element in the initial ranking score. For example, let’s consider the _Rs_ ranking and the top-3 ranked configurations are {c1,c2,c3}, that overlaps only with the bottom-3 ranked configurations in query Q. That is, {c4,c2,c5}, i.e c2 is in the 59th position out of 60 ranks/positions (i.e., the rank before last). Thus, A(R) = 1 − 1/(11 ∗ 3), when k = 3 and Q = 11.<br>

- __Coherence__ <br>
: Ranking coherence is a measure of the number of dis(agreements) using _Kendall's Index_ between two ranking sets that uses the same ranking criteria across different experiments
<p>
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/coherenceFormula.png"/>
</p>
In this experiment, we assume that rank sets have the same number of elements. Kendall’s distance between two rank sets R1 and R2, where P represents the set of unique pairs of distinct elements in the two sets. For instance, the K index between R1={c1,c2,c3} and R2={c1,c2,c4} for 100M and 250M is 0.33, i.e., one disagreement out of three pair comparisons.