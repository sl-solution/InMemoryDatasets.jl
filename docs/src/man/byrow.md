# Function `byrow`

## Introduction

The `byrow` function is a high performance (multi-threaded) function for row-wise operations. It is designed to make tasks like summing up each row simple, efficient, and lightening fast. The function can be used as a stand-alone function or inside `modify`/`modify!` or `combine` functions. The stand-alone syntax of the function is `byrow(ds, fun, cols, ...)`, where `ds` is a data set, `fun` is a function, and `cols`  is the list of columns which row-wise operation is going to be applied on their values in each row, e.g. the following code creates a data set with 100,000 rows and 100 columns, and adds the values in each row,

```jldoctest
julia> ds = Dataset(rand(10^5, 100), :auto);
julia> byrow(ds, sum, 1:100)
100000-element Vector{Union{Missing, Float64}}:
 50.655934293702366
 51.481108371018266
 51.27498152964299
 54.097941432844536
 52.28727157779627
 56.215091415376975
 53.940023864095856
 47.65424080373157
  ⋮
 46.360565247921
 45.91721147194705
 52.047072415296824
 48.71125460530455
 50.82102574082131
 49.90462723123929
 46.594683329278816
 50.47529543725829
```

As it can be observed the function syntax is very straightforward, and to examine the efficiency of it, we use the `@btime` macro from the BenchmarkTools package,

```jldoctest
julia> using BenchmarkTools
julia> @btime byrow(ds, sum, 1:100);
  7.874 ms (2143 allocations: 1.02 MiB)
julia> m = Matrix(ds)
julia> @btime sum(m, dims = 2)
  20.773 ms (7 allocations: 879.11 KiB)
```

In the above benchmark, `byrow` should be even more performant when the data set has a group of heterogeneous columns.

## Optimised operations

Generally, `byrow` is efficient for any `fun` which returns a single value for each row, however, it is fine tuned for the following functions:

* `all` : Test whether all elements of a boolean collection are `true`
* `any` : Test whether any elements of a boolean collection are `true`
* `argmax` : Return the column name of the maximum element
* `argmin` : Return the column name of the minimum element
* `coalesce` : Return the first value which is not equal to `missing`
* `count` : Count the number of `trues`
* `findfirst` : Return the column name of the first true value
* `findlast` : Return the column name of the last true value
* `hash` : Compute an integer hash code
* `isequal` : Return `true` when all values are equal
* `issorted` : Check if the values are sorted
* `maximum` : Return the maximum value
* `mean` : Compute the mean value
* `minimum` : Return the minimum value
* `nunique` : Return the number of unique values
* `prod` : Return the product of values
* `select` : Select values of specific columns in each row. The specific columns can be passed using `by = scols`, where `scols` can be a vector of columns names or a column name of the passed data set.
* `std` : Compute the standard deviation of values
* `sum` : Return the sum of values
* `var` : Compute the variance of values

The common syntax of `byrow` for all of these functions except `nunique`, `coalesce`, `isequal`, and `issorted` is:

`byrow(ds, fun, cols; [by , threads = true])`

The `by` keyword argument is for specifying a function to call on each value before calling `fun` to aggregate the result, and `threads = true` causes `byrow` to exploit all cores available to Julia for performing the computations.

The `nunique` function doesn't accept `threads` argument, however, it has an extra keyword argument `count_missing`. `nunique` counts the number of unique values of each row, and `count_missing = true` counts missings as a unique value.

The `coalesce`, `isequal`, and `issorted` functions don't accept `by` argument, however, `issorted` accepts extra keyword argument `rev` which is set to `false` by default.


### Examples

Let's first create an example data set which we will use for the rest of this section:

```jldoctest
julia> ds = Dataset(g = [1, 1, 1, 2, 2],
                    x1_int = [0, 0, 1, missing, 2],
                    x2_int = [3, 2, 1, 3, -2],
                    x1_float = [1.2, missing, -1.0, 2.3, 10],
                    x2_float = [missing, missing, 3.0, missing, missing],
                    x3_float = [missing, missing, -1.4, 3.0, -100.0])
5×6 Dataset
 Row │ g         x1_int    x2_int    x1_float   x2_float   x3_float
     │ identity  identity  identity  identity   identity   identity
     │ Int64?    Int64?    Int64?    Float64?   Float64?   Float64?
─────┼───────────────────────────────────────────────────────────────
   1 │        1         0         3        1.2  missing    missing
   2 │        1         0         2  missing    missing    missing
   3 │        1         1         1       -1.0        3.0       -1.4
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0
```

To compute the mean of each row for the float columns, we simply call,

```jldoctest
julia> byrow(ds, mean, r"_float")
5-element Vector{Union{Missing, Float64}}:
   1.2
    missing
   0.20000000000000004
   2.65
 -45.0
```

Note that, since for the second row all values are `missing`, the result of mean is also `missing`.

To calculate the mean of the absolute value of each row for the float columns we use the same code and pass `by = abs` as the keyword argument,

```jldoctest
julia> byrow(ds, mean, r"_float", by = abs)
5-element Vector{Union{Missing, Float64}}:
  1.2
   missing
  1.8
  2.65
 55.0
```

To find rows which all their values are greater than 0 in the first three columns we can use the following code,

```jldoctest
julia> byrow(ds, all, 1:3, by = x -> isless(0, x))
5-element Vector{Bool}:
 0
 0
 1
 1
 0
```

Note that in Julia `isless(0, missing)` is `true`.

To find rows which contain at least one missing value in any of the columns we can use the following code,

```jldoctest
julia> byrow(ds, any, :, by = ismissing)
5-element Vector{Bool}:
 1
 1
 0
 1
 1
```

It means that except the third row, all other rows contain missing values. Using `byrow` with `count` function, we can count the number of non-missing values in each row,

```jldoctest
julia> byrow(ds, count, :, by = !ismissing)
5-element Vector{Int32}:
 4
 3
 6
 4
 5
```

In the following example, in each row we pick the values of selected columns passed by the `by` keyword argument.

```jldoctest
julia> ds = Dataset(x1 = 1:4, x2 = [1,2,1,2], NAMES = [:x1, :x2, :x1, :x1])
4×3 Dataset
 Row │ x1        x2        NAMES    
     │ identity  identity  identity
     │ Int64?    Int64?    Symbol?  
─────┼──────────────────────────────
   1 │        1         1  x1
   2 │        2         2  x2
   3 │        3         1  x1
   4 │        4         2  x1

julia> byrow(ds, select, r"x", by = :NAMES)
4-element Vector{Union{Missing, Int64}}:
 1
 2
 3
 4

julia> byrow(ds, select, r"x", by = ["x1", "x2", "x2", "x2"])
4-element Vector{Union{Missing, Int64}}:
 1
 2
 1
 2

julia> byrow(ds, select, [:x2, :x1], by = [1,2,2,1])
4-element Vector{Union{Missing, Int64}}:
 1
 2
 3
 2
```

In the last example, note that the integers in `by` are mapped to the corresponding columns passed to the function, i.e. 1 is referring to `:x2` (it is the first column passed to the function as the column selector) and 2 is referring to `:x1`.


## `mapreduce`

One special function that can be used as `fun` in the `byrow` function is `mapreduce`. This can be used to implement a customised reduction as row operation. When `mapreduce` is used in `byrow`, two keyword arguments must be passed, `op` and `init`. For example in the following code we use `mapreduce` to `sum` all values in each row: (note that unlike `byrow(ds, sum, :)` the following function will return missing for a row if any of the value in that row is missing)

> `byrow(ds, mapreduce, :, op = .+, init = zeros(nrow(ds)))`

## User defined operations

For user defined functions which return a single value, `byrow` treats each row as a vector of values, thus the user defined function must accept a vector and returns a single value. For instance to calculate `1 * col1 + 2 * col2 + 3 * col3` for each row in `ds` we can define the following function:

```jldoctest
julia> avg(x) = 1 * x[1] + 2 * x[2] + 3 * x[3]
avg (generic function with 1 method)
```

and directly use it in `byrow`,

```jldoctest
julia> byrow(ds, avg, 1:3)
5-element Vector{Union{Missing, Int64}}:
 10
  7
  6
   missing
  0
```

Note that `avg` is missing if any of the values in `x` is missing.

## Special operations

`byrow` also supports a few optimised operations which return a vector of values for each row. The `fun` argument for these operations is one of the followings:

* `cummax`
* `cummax!`
* `cummin`
* `cummin!`
* `cumprod`
* `cumprod!`
* `cumsum`
* `cumsum!`
* `fill`
* `fill!`
* `sort`
* `sort!`
* `stdze`
* `stdze!`

The main difference between these operations and the previous operations is that these operations return a data set with the corresponding row has been updated with the operation. For the operations with `!` the updated version of the original data set is returned and for the operations without `!` a modified copy of the original data set is returned.

The `fill` and `fill!` functions fill missing values (or any other values which a function passed to the `condition` keyword argument returns `true`) in each row of a given data set by given values passed to the `by` keyword argument. The function passed to the `condition` keyword argument must return `true` or `false`.

> Note that for the `fill` and `fill!` functions the filling happens in-place, thus, if this is not possible Julia will throws errors.

```jldoctest
julia> ds = Dataset(x1 = [missing, 2, 1], x2 = [1, missing, missing], y = [4,5,3])
3×3 Dataset
 Row │ x1        x2        y        
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │  missing         1         4
   2 │        2   missing         5
   3 │        1   missing         3

julia> byrow(ds, fill, 1:2, by = :y)
3×3 Dataset
 Row │ x1        x2        y        
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │        4         1         4
   2 │        2         5         5
   3 │        1         3         3

julia> byrow(ds, fill, 1:2, by = [0,2,1])
3×3 Dataset
 Row │ x1        x2        y        
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │        0         1         4
   2 │        2         2         5
   3 │        1         1         3

julia> byrow(ds, fill, 1:2, by = [0,2,1], condition = x->ismissing(x) || isequal(x, 1))
3×3 Dataset
 Row │ x1        x2        y        
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │        0         0         4
   2 │        2         2         5
   3 │        1         1         3
```

the cumulative functions calculate the cumulative min, max, sum, and product, `sort` sorts the values in each row, and `stdze` standardises the values in each row. The `sort` operation accepts all keyword arguments that the function `sort` in Julia Base accept.

```jldoctest
julia> byrow(ds, cumsum, 1:3)
5×6 Dataset
 Row │ g         x1_int    x2_int    x1_float   x2_float   x3_float
     │ identity  identity  identity  identity   identity   identity
     │ Int64?    Int64?    Int64?    Float64?   Float64?   Float64?
─────┼───────────────────────────────────────────────────────────────
   1 │        1         1         4        1.2  missing    missing
   2 │        1         1         3  missing    missing    missing
   3 │        1         2         3       -1.0        3.0       -1.4
   4 │        2         2         5        2.3  missing          3.0
   5 │        2         4         2       10.0  missing       -100.0
```

Note that for these operations, by default, `cumsum` treats `missing` as zero, and `cumprod` treats `missing` as one, i.e. they ignore `missing` values, however, passing `missings = :skip` causes these functions to skip the missing values (leave them as `missing`). For other cumulative functions the same keyword argument rules the behaviour.

The special operations don't change the columns names or their orders.
