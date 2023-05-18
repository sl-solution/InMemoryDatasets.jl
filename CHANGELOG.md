# Dev

## Fixes

* Fix a problem that was causing tests fail in Julia 1.9
* Fix an issue with `eltype` and the output of `eachcol`. Now `eltype(::Type{<:DatasetColumns})` properly returns `AbstractDatasetColumn` instead of `AbstractVector`.
* Fix a problem with `nonmissingtype` with `Union{}` output.
* Fix an issue that was causing the join functions sort already-sorted data sets, [issue #108](https://github.com/sl-solution/InMemoryDatasets.jl/issues/108)
* Remove precompilation for Julia 1.9 - it causes enormous amount of allocation in precompiling and loading 

## Features

* Now `IMD` throws errors when accesses a grouped data set which its parent is modified.

# Version 0.7.15

## Fixes

* Functions `searchsorted`, `searchsortedfirst`, and `searchsortedlast` now works with `DatasetColumn`
* Fix a bug in `byrow(nunique)`

# Version 0.7.14

## Fixes

* Fix a bug which caused `stable=true` being ignored in `gatherby`, [issue #100](https://github.com/sl-solution/InMemoryDatasets.jl/issues/100)

## Documentation

* Add docstring for `groupby!`, `groupby`, and `gatherby`.

# Version 0.7.13

## Fixes

* Fix issue with `QuickSortAlg` in future version of Julia
* Empty the rows of a `SubDataset` without columns
* Fix a bug which causes `modify/combine` throw errors on columns with Vector{Vector} type

# Version 0.7.12

## Features

* Users can use `resize!` to resize a data set

## Fixes

* Fix function signature for some stat functions

# Version 0.7.11

## Features

* Update to `PrettyTables` version 2

## Fixes

* Fix a but in `byrow` for writing values of type `BigInt`
* Update for `Julia` VERSION >= v"1.9.0-DEV.1635"
* Fix a bug in `modify` which causes an error to show an error!
* Fix a bug in `sort` which causes to treat `Bool` as a vector with length 1
# Version 0.7.10

## Fixes

* `topk` and `topkperm` use `isless` by default for comparing values.
* Fix a bug in `show` which causes ignoring format of a column when calculating the max width.
* Better `show` for `GroupBy/GatherBy` in Jupyter
* `hcat!` keeps the format of the second data set.
* Fix an issue in show with HTML MIME, [issue #91](https://github.com/sl-solution/InMemoryDatasets.jl/issues/91)


## Performance

* Now `Jupyter` shows very wide data sets much faster, [issue #82](https://github.com/sl-solution/InMemoryDatasets.jl/issues/82)
* Add precompilation for Julia > 1.8
# Version 0.7.8

## New features

* The `topk` and `topkperm` functions supports two extra arguments: `lt` and `by` which by default are set as `<` and `identity`, respectively
* `topkperm` is a new function for outputting the indices of top(bottom) k values [issue #67](https://github.com/sl-solution/InMemoryDatasets.jl/issues/67).
* `topk` now supports any `DataType`, see [issue #67](https://github.com/sl-solution/InMemoryDatasets.jl/issues/67).
* `filter`, `filter!`, `delete` and `delete!` have a new keyword argument for controlling how the missing values should be interpreted [issue #69](https://github.com/sl-solution/InMemoryDatasets.jl/issues/69)

## Fixes

* `topk` now works on `DatasetColumn` / `SubDatasetColumn`.

* Stats functions throw `ArgumentError` when an empty vector is passed to them.

## Performance

* The `topk` and `topkperm` functions are multithreaded ready, i.e. users can pass `threads = true` to these functions.
  
  * Now we use binary search for large values of k. This improves the performance of the functions in the worst case scenarios.

* `row_join!` allocates less when `mapformats=true`, thus, performs better. This directly affects `filewriter` performance in `DLMReader`.

* # Version 0.7.7

## New features

* A new functionality has been added to `byrow` for passing a Tuple of column indices. `byrow(ds, fun, cols)` calls `fun.(ds[:, cols[1]], ds[:, cols[2]], ...)` when `cols` is a NTuple of column indices.

## Fixes

* Fix type ambiguity in `filter/!`

# Version 0.7.6

## New features

* Two new functions: `delete` and `delete!`. They should be compared to `filter` and `filter!`, respectively - [issue #63](https://github.com/sl-solution/InMemoryDatasets.jl/issues/63)
* Add `DLMReader` to `sysimage` in `IMD.create_sysimage`.

## Fixes

* Fix mistakes in `byrow(argmin)` and `byrow(argmax)` - [pull #62](https://github.com/sl-solution/InMemoryDatasets.jl/pull/62)

# Version 0.7.4 - 0.7.5

## New features

* `byrow(ds, t::DataType, col)` convert values of `col` to `t`. 

## Fixes

* Fix an issue in `flatten/!` - columns with type `Any`.
* Fix an issue with `IMD.create_sysimage` - [issue #59](https://github.com/sl-solution/InMemoryDatasets.jl/issues/59)
* Improve `eachgroup`
* Drop support of `UInt16` in `Characters` - `Characters` now only supports length

# Version 0.7.3

## New features

* Users now can choose between having the observations ids for the left data set and/or the right data set as part of the output data set.
* Add a new function `eachgroup`. It allows iteration over each group of a grouped data set.
* `op` is a new keyword argument for the `update/!` functions which allows passing a user defined function to control how the value of the main data set should be updated by the values from the transaction data set. ([issue #55](https://github.com/sl-solution/InMemoryDatasets.jl/issues/55))
* Supporting of the `mapformats` keyword argument in `flatten/!`. Now users can flatten a data set based on the formatted values. ([issue #57](https://github.com/sl-solution/InMemoryDatasets.jl/issues/57))
* Support of the `threads` keyword argument in `flatten/!`.

## Fixes

* The `combine` function will now work fine when a view of data set is passed
* For the join functions the `makeunique` argument is now passed correctly to the inside functions.
* `update` and `update!` have the same `mode` option by default.
* Fix the problem with preserving format of `SubDataset` in `flatten/!`
* Fix the problem that caused `flatten!` to produce a copy of data when an empty data set were passed to it.
* Fix the bug in `flatten!` related to flatten the first column.
* Fix the bug in `flatten` that caused Segmentation fault for view of data sets.

## Performance

* Faster `flatten/!`

# Version 0.7.0

## New features

* The `outerjoin` function accepts the `source` keyword argument.
* All join functions support `obs_id` option. This allows to output obs id for the matched pairs.
  * All join functions support `obs_id_name` for assigning column names for `obs_id`.
* The `leftjoin/!`, `innerjoin` and `outerjoin` functions support `multiple_match` option. This indicates the rows in the left data set that has been repeated in the output data set due to multiple matches in the right data set.
  * All join functions support `multiple_match_name` for assigning the column name for `multiple_match`.
* The `compare` function is updated to support more complex comparisons([issue #53](https://github.com/sl-solution/InMemoryDatasets.jl/issues/53)).
  * [BREAKING] the `on` keyword argument in previous versions is equivalent to the `cols` keyword argument in version 0.7.0+.
  * The `compare` function can compare two data sets with different number of rows.
  * User can pass key columns to `compare`, via the `on` keyword argument, for matching observations before comparing.
  * Few keyword arguments are added to `compare` for supporting new functionalities.

## Fixes

* The `maximum` and `minimum` functions now work properly with `String` columns.
