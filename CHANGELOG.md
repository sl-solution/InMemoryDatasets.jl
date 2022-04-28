# Dev

## New features

* Users now can choose between having the observations ids for the left data set and/or the right data set as part of the output data set.
* Add a new function `eachgroup`. It allows iteration over each group of a grouped data set.

## Fixes

* The `combine` function will now work fine when a view of data set is passed
* For the join functions the `makeunique` argument is now passed correctly to the inside functions.

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
