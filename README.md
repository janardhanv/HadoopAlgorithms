HadoopAlgorithms
================

Hadoop algorithms implementation for my MSC' thesis
================

Algorithm based on Query-dependent Sorting
================
This algorithm (termed SORT-QD) works in two phases (two MapReduce jobs). In the rst phase,
sorting of the data tuples  in R is performed according to the score s( ), which is produced based
on the weighting vector w of query qk(w). In the second phase, it suces to scan the rst k tuples
only and report them as result, since the tuples are ranked based on the score of the query at
hand. Obviously, this algorithm requires no pre-processing, but during query processing it needs
to sort the relation R for each query.
2To do the sorting, we can leverage the shue mechnism of Hadoop that performs sorting based
on key for each Reducer. What we need to do is output key-value pairs from the Mapper, with
key the score of the tuple and value the entire tuple. If we specify that only one Reducer will be
used, then a single sorted le of tuples in R will be generated. If we use multiple Reducers (say
r), then r individually sorted les (parts) of R will be generated on disk.
