# Parquet.Producers

The idea behind this project is to take inspiration from classic [MapReduce](https://en.wikipedia.org/wiki/MapReduce), a still widely-used framework for processing very large amounts of data, but to simplify it by removing special framework features without loss of capability, ensuring that there is a "user-space" solution, i.e. the remaining features can still be assembled conveniently to achieve the same ends.

Where a MapReduce framework may cluster data by key without necessarily needing to fully sort them, here we are particularly interested in producing truly sorted output in Parquet, given its usefulness in other operations such as single-pass joins.

Also we are interested in supporting incremental updating, such that the sorting workload per-update is minimised.

## Background: Classic MapReduce

In classic [MapReduce](https://en.wikipedia.org/wiki/MapReduce) an application processes very large amounts of data across multiple nodes by defining a pair of functions:

-   `Map`: this accepts a single source key-value pair, $(K_s, V_s)$ and outputs zero or more mapped $(K_m, V_m)$, where the data types used in the source and mapped pairs needn't be the same.
-   `Reduce`: this accepts a single $K_m$ and a list $[V_m]$, the values in the list all having been emitted by `Map` with the same key, and outputs zero or more $(K_r, V_r$) pairs, again with no requirement that these match the types of the input or the mapped pairs.

The framework is responsible for sorting the output of `Map` by the output $K_m$, so that adjacent groups of $V_m$ with the same $K_m$ are ready to be sent on to `Reduce`. The work of calling `Map` can easily be distributed across multiple nodes.

It is not explicitly stated whether the call to `Reduce` for some $K$ will receive the complete list $[V]$ for that $K$. By default this is the case; typically the $K$ alone is hashed to select a reducer node, and so all output with the same key will be sent to the same node for the `Reduce` stage to work on it.

But it is possible to configure the algorithm for choosing the reducer node for each $(K, V)$, e.g. by examining the $V$ instead of the $K$. This will splits the values for a given $K$ into subsets, which are passed to `Reduce` separately (although of course each call still only receives values with the same $K$). The purpose of this is to allow greater parallelism, although it means that further processing may be needed on the output to aggregate the partially-aggregated results.

Applications may involve several separate `Map`/`Reduce` stages, each stage working from the results of the previous. The classic framework does not support incremental updating; repeat processing is carried out from scratch.

## Generalised Producers

If the signature of `Map` was to be modified to accept a list of source values associated with a source key, $(K_s, [V_s])$, rather than a single $V$, it would be the same as `Reduce`. A single function called `Produce` would take the place of both types of function, emitting target pairs $(K_t, V_t)$. The user can define a sequence of chained producers in this way, so the standard `Map`/`Reduce` pattern becomes a special case.

As before the framework provides the "glue" that makes producer stages fit together naturally. The output of a producer is sorted by target $K_t$, which makes it easy for a subsequent stage to be presented with a single $K_t$ and a list $[V_t]$ of all the values that were emitted with that key $K_t$. In other words, the target format of one stage becomes the source format of subsequent stage.

## Partitioning By Key

It is not strictly necessary to have a configurable algorithm for partitioning: partitioning by key is always sufficient. The user can always emit a compound key to achieve partitioning. Example: we want to know the average salary by age range for persons across an entire country. First solution:

1. Producer that receives `person` records in arbitrary batches and emits `(age_range, salary)` per `person`
2. Producer that receives an `age_range` key and a list of `salary` values and emits `(age_range, mean(salary))`

Suppose that stage 2 is guaranteed to receive _all_ salaries for the given `age-range` (key-based partitioning), so can safely compute the average of them. The downside is that there may only be ~10 distinct age-ranges and we have 100 nodes available to share work. Second solution:

1. Producer that receives `person` records in arbitrary batches and emits `((age_range, R), salary)` per `person`, where `R` provides the second part of a compound key `(age_range, R)`. `R` is simply a random number between 0 and 9.
2. Producer that receives an `(age_range, R)` key and a list of `salary` values and emits `(age_range, (count(salary), sum(salary)))`, discarding `R`, but also including the count as well as the sum of salaries in the output value.
3. Producer that receives an `age_range` key and list of `(count_salary, summed_salary)` pairs, and emits `(age_range, sum(summed_salary)/sum(count_salary))`, thus finding the final average per age-range.

Thus by optionally including a partitioning factor in the key, we retain the ability to split work between nodes. Note that the "tricky" part of solving this problem (realising that you can't simply average a set of averages obtained from subsets, and you must know the count per subset as well, i.e. knowing what an associative operator is) is ever-present in classic MapReduce. The concept of a custom partitioner does not solve that problem; it only adds feature-baggage to the framework without enabling any capability that cannot already be solved quite easily in user-space.

## Initial Input

In classic MapReduce there is an _input reader_ stage, which is responsible for presenting $(K, V)$ pairs to the framework so it can pass them to the first `Map` function. No requirements are usually imposed on the ordering of this data.

Here we introduce a rule that this initial set of data must be ordered by $K$, because the interface to `Produce` is a $K$ accompanied by all values with that $K$. The simplest way to persist the source data will be records of $(K, V)$ pairs with the same $K$ repeated on clusters of adjacent rows ("flat" format), and one (though not the only) way to arrange such clustering is to sort by $K$.

At first glance this imposes a difficult responsibility on the user: pre-sorting the data set, which may be very large. But the framework can take care of this. The list provided to `Produce` is a single-use forward-iterable _lazy_ sequence, and thus does not need to be all held in memory, so it can be arbitrarily large, i.e. it could be the entire input dataset, all associated with the same arbitrary key. The user can therefore pass into the framework a sequence of pairs `(1, person)`, i.e. the constant key $1$ for every `person` record. This trivially meets the requirement of being sorted by $K$, there being only a single distinct $K$. The `Produce` function will be called once, receiving a $V$ containing all the `person`s, and can then emit with whatever $K$ from `person` that would be most useful to sort by, and thus the framework takes care of all large-scale sorting responsibilities.

Example: we have an unsorted file of PII and we want to find all email groups of persons who claim to have the same email address. This is an example of a problem where the entire list associated with a given key must be presented to a single `Produce` call.

1. Application lazily streams `person` records from a file and, combines each with the key $1$ on-the-fly and presents the resulting stream of `(1, person)` to the framework as input
2. Producer receives a $K$ that it can ignore (it's always `1`) and a `[person>]` list, and emits `(email, person)` per person
3. Producer receives a $K$ that is the `email` and a list of `person`s with that email. If the list contains two or more persons it emits `(email, person)`, otherwise it emits nothing.

Step 2 uses the framework to take care of the sorting problem.

## Merging

If two producers emit compatible $(K_t, V_t)$ pair types, their output can be quite easily parallel merged to produce a single dataset which can be efficiently fed into a third producer stage.

## Joins

A join uses a `Produce` function that takes two more source groups, so accepts a tuple $(K_s, [V_s], [{W_s}], \dots)$. This means that the sources must have the same key type, $K_s$, but they can have different value types $V_s$, $W_s$ and so on.

The `Produce` function described up to this point is simply a special case where there is only one source.

Note that as the lists of values provided to `Produce` are implemented as single-use forward-iterable lazy sequences, the only way to produce the cartesian product of two sets of values is to make a copy in memory of one of the lists first. This may not be much of a limitation in practise; often the problem to be solved involves a one-to-many relationship, in which case only one value from one list has to be cached for reuse across the other list. In any case, these concerns are left to the author of the `Produce` function.

## Monadic Interpretation

_Note: this section can be skipped._

The formal way to define a pipeline of chained transformations is through monads. A monad consists of three things:

-   A type `M<T>` that "amplifies" an underlying type `T` (e.g. `T[]`, `Promise<T>`)
-   A `unit` operation that converts a plain `T` into an `M<T>` (e.g. `[x]`, `Promise.resolved(x)`)
-   A `bind` operation that takes a user-supplied function `F: T -> M<R>` and an instance of `M<T>` and returns an `M<R>`, where `R` is an underlying type that need not be the same as `T`.

The `bind` may perform characteristic computation of its own, e.g. `T[]` can contain multiple `T`, so once it has passed all the `T` through `F`, it has multiple `R[]` results, so it concatenates them into a single `R[]` (this operation is called `flatMap` in several platforms).

Now perform this conceptual substitution:

-   The `T` is a $(K_s, [V_s])$, i.e. a source key and all the source values associated with that key.
-   The `M<T>` is the complete dataset of all the groups presented as input to the framework, which must be ordered by the groups' keys.
-   The `unit` operation creates a dataset with a single group and an arbitrary $K_s$ (this is the initial input sorting trick discussed above.)
-   The user-supplied function `F` is the `Produce` function (but see note below.)
-   The `bind` operation is the framework-supplied logic that acts on each group with the `Produce` function, and sorts by the output keys to form new groups for the output `M`.

Note that for convenience the user doesn't have to return groups from their `Produce` function in the correct order by $K_t$. They can just emit $(K_t, V_t)$ pairs in any order. If these were sorted by $K_t$ and then grouped, they would be a true $M$, but the framework is going to have to sort the output obtained from multiple separate `Produce` calls anyway, so there is no advantage (and huge disadvantage) in making life difficult for the author of a `Produce` function.

## Implementation 1 (Non-Incremental)

These two representations are clearly very easy to convert between on-the-fly:

-   _flat_: a list of $(K, V)$ sorted by $K$
-   _grouped_: a list of groups having a $K$ and a $[V]$, the groups being sorted by $K$

They are so close to being the same thing, the difference between them is a matter of interpretation.

We use [Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) files with a schema divided into $K$ and $V$ columns, with rows sorted by $K$, i.e. the flat format, but we can very easily interpret it as the grouped format. Parquet performs compression on columns, so a long run of repeated identical keys will take up very little space.

The `Produce` function is only allowed to forward-scan the $[V]$ list once. Suppose we have a _cursor_ abstraction over a lazy sequence that points at the current record and allows us to step forward to the next record until the values are exhausted. We obtain such a cursor across the entire input dataset, and wrap it in a bounded cursor, which:

-   only exposes the $V$ of each underlying pair, and
-   claims to be exhausted when the $K$ changes.

This wrapped cursor can be passed to the `Produce` function along with the real cursor's current $K$. Thus we proceed through the entire input dataset calling `Produce` multiple times, once for each $K$.

The output $(K, V)$ pairs emitted by `Produce` can be buffered in memory up to some large number, sorted by $K$ and saved in one or more partial Parquet files. Once the whole dataset has been processed a merge sort can be performed to get the final state of the step. That final state is now in the flat format, and is ready to be forward-scanned by a subsequent stage.

## Incremental Updating

An incremental update is a subsequent processing run that accepts a new set of $(K_s, V_s)$ pairs that are a subset of the whole source dataset, replacing the previous pairs for those keys.

It is only possible to replace all values for a given key. That is, if the initial input included a million values with key $x$, then an update only includes three values with $x$, those three values will replace all of the million previous values associated with $x$. In summary, any key mentioned in the update will be entirely replaced by the information in the update, whereas any key not mentioned is unaffected.

It follows that the first stage's source should have some level of key granularity or else incremental updating is impossible. Example: we receive daily batches of records. Therefore the date could serve as the key that is associated with all the records in the daily file, so no pre-sorting logic is necessary, but it is then possible to correct all the data for a specific day (or set of days) in a subsequent update.

We also need to be able to delete the values associated with a key, without providing new values. So we extend the input format to be $(K, V?)$ i.e. the $V$ is optional, and the absence of a $V$, as in `('foo', null)`, means that the $K$ `'foo'` has been removed from the input since the previous update.

To avoid ambiguity, we require that the source data for an incremental update must contain only one of the following per key `k`:

-   a single deletion pair `(k, null)`
-   any number of upsert pairs `(k, v1)`, `(k, v2)`, ...

That is, there is no mixing of deletions and upserts for the same key. There is never a need to mix them because the presence of any upsert for a key automatically implies the deletion of all previous values for that key. The only reason for sending a deletion pair for some key `k` is because there are now no values associated with `k`.

## Implementation 2 (Incremental)

To implement this pattern, the framework needs to store more information. The challenge is that a source value $V_t$, which is associated with a source key $K_s$, can contribute to any target pair $(K_t, V_t)$ according to the whim of the author of the `Produce` function.

The following scheme is adopted. The Parquet containing the ultimate result of a producer (known as the _content_) has the logical schema $(K_t, K_s, V_t)$, and is sorted by $(K_t, K_s)$, i.e. by target key and then by source key. So we track which source key produced each target key and value. This alone would be sufficient for an unpredictably expensive solution.

In addition we store a second Parquet file known as the _key-mappings_, with logical schema $(K_s, K_t)$, sorted by $(K_s, K_t)$, i.e. by source key and then by target key, but no values.

The framework is thus able to perform a single-pass parallel scan of all the incoming update pairs (which are sorted by $K_s$) and the key-mappings (also sorted by $K_s$). It can fast-forward through the key-mappings to find any associated with the current $K_s$ and discover the set of $K_t$ that need to be removed.

The output of this discovery process, combined with the output of `Produce`, is a set of instructions for how to update the target data. It consists of deletions and "upserts" (inserts or updates).

It is this set of instructions that must be sorted, in contrast to the first implementation that sorted the entire dataset. Two different versions of the instruction set are created, one of $(K_t, K_s, V_t)$ and the other of $(K_s, K_t)$, and then once all the instructions have been produced and sorted, we are ready to produce updated versions of the full content and key-mappings files, which only requires parallel scans.

This design is predicated on these assumptions:

-   Performing forward scans through datasets is a comparatively fast operation
-   Sorting datasets is a comparatively slow operation
-   Incremental updates will affect a small subset of the keys in each stage

Therefore it only sorts the intermediate sets of instructions describing how the content and key-mappings are to be updated, for the affected $(K_s, K_t)$ combinations.

That is, suppose the existing data has these content records $(K_t, K_s, V_t)$:

-   `(105, 6, 'apple')`
-   `(105, 9, 'banana')`
-   `(108, 4, 'mango)`

And therefore these key-mappings, $(K_s, K_t)$:

-   `(4, 108)`
-   `(6, 105)`
-   `(9, 105)`

The update replaces the values for source key `6`. As the update is scanned in parallel with the key-mappings, we discover that `6` previously produced a target key `105` and so we generate an intermediate instruction to delete `(6, 105)` from the key mappings and to delete `(105, 6, *)` from the content. None of this has any effect on the record that matches `(105, 9, *)`.

An update operation is like a transaction that involves updating every producing stage once. Stages form a directed acylic graph, so the stages that accept external updates are updated first, then the stages that feed from them, and so on through the whole graph. The states of the whole dataset (consisting of multiple producers) can therefore be thought of as a series of versions `1, 2, 3...`. In each version every producer has three parquet files: content, key-mappings and update. To produce version $N + 1$, every stage reads the version $N$ files of itself and its source stages.

## Incremental Multiple Stages

It follows that any subsequent processing stages, which maintain their own content and key-mappings from previous updates, do not want to consume the entire dataset from a previous stage. Instead, they want an incremental update in the form of a sequence of $(K, V?)$ where a `null` in $V$ means that the $K$ has been deleted, and otherwise the set of adjacent pairs with the same key includes every value for that key.

Therefore there is an additional output available from a producing stage, known as the _update_, which is precisely that set of $(K_t, V_t?)$ pairs describing the changes just made to its target content.

This can be fed into subsequent stages as the $(K_s, V_s)$ so they can incrementally update.

Note that this update is not identical to any previously described dataset such as the intermediate instructions. We noted above that if the source key `6` has changed, and the previous content was:

-   `(105, 6, 'apple')`
-   `(105, 9, 'banana')`

This only requires the intermediate instructions to refer to the row(s) matching `(105, 6, *)`, i.e. with the same source key.

Whereas a subsequent stage needs to be passed the $(K_t, V_t?)$ (the first and third columns) from all the rows matching `(105, *, *)`, because it doesn't care what the previous stage's source keys are (that is an internal detail of the previous stage). It requires the _full_ set of key-value pairs for each affected key.

## Incremental Merging of Multiple Previous Stages

We previously mentioned how simple it is to do a parallel merge across multiple prior stages with the same key-value types and present the merged data to a subsequent stage.

This is less simple under incremental merging. The solution has three layers to it. We refer to the prior stages as _feeders_:

1. Produce the combined distinct list of all keys that were affected by the current update, across all feeders. This is the list of _updated keys_.
2. In each feeder, set up a three-way parallel scan between the updated keys, the feeders's content and the feeders's update. For each key in the updated keys, if the feeder has updates for that key, emit them, otherwise emit all the feeder's content for that key _as if they were updates_. The resulting sequence is the _augmented updates_ for the feeder.
3. Perform a parallel merge across the augmented updates from all the feeders, which is the effective merged update, This layer must take care to emit _either_ a single deletion _or_ one or more upserts per key.

Why does each feeder's augmented updates sequence need to potentially include a mixture of updates and static content? Because when the consuming `Produce` function is fed the values for a key, the list must include all the values for that key.

## Incremental Joins

It was noted above that a `Produce` stage can accept multiple sources with different value types, thus acting as a join. This concept also requires revisiting in the face of incremental updates.
