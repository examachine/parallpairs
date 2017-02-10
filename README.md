# parallpairs

## Parallel all-pairs similarity search algorithms in OCaml

If you use this code, please cite the following paper. It is currently under review at IJPP.

https://arxiv.org/abs/1402.3010

You may cite it as:

Eray Özkural, Cevdet Aykanat: 1-D and 2-D Parallel Algorithms for All-Pairs Similarity Problem. CoRR abs/1402.3010 (2014)
http://dblp.org/rec/html/journals/corr/OzkuralA14

1-D and 2-D Parallel Algorithms for All-Pairs Similarity Problem

Eray Özkural, Cevdet Aykanat
(Submitted on 13 Feb 2014)

All-pairs similarity problem asks to find all vector pairs in a set of vectors the similarities of which surpass a given similarity threshold, and it is a computational kernel in data mining and information retrieval for several tasks. We investigate the parallelization of a recent fast sequential algorithm. We propose effective 1-D and 2-D data distribution strategies that preserve the essential optimizations in the fast algorithm. 1-D parallel algorithms distribute either dimensions or vectors, whereas the 2-D parallel algorithm distributes data both ways. Additional contributions to the 1-D vertical distribution include a local pruning strategy to reduce the number of candidates, a recursive pruning algorithm, and block processing to reduce imbalance. The parallel algorithms were programmed in OCaml which affords much convenience. Our experiments indicate that the performance depends on the dataset, therefore a variety of parallelizations is useful.


The code is quite interesting, as it shows how to effectively use OCaml for MPI programming. There is a bunch of well-written parallel functional code that I will extract from this codebase and release separately. You need the latest ocamlmpi release as that contains the patches I made to make this code work.

You can download the datasets from: https://github.com/examachine/data/tree/master/dv


Eray Ozkural, PhD
