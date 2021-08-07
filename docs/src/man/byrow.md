# Row-wise operations
## `byrow` function

The `byrow` function is a high performance (multi-threaded) function for row-wise operations. It is designed to make tasks like summing up each row simple, efficient, and lightening fast. The function can be used as a stand-alone function or inside `modify` or `combine` functions. The stand-alone syntax of the function is `byrow(ds, fun, cols, ...)`, where `ds` is a data set, `fun` is a function, and `cols`  is the list of columns which row-wise operation is going to be applied on their values in each row, e.g. the following code creates a data set with 100,000 rows and 100 columns, and adds the values in each row,

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

As it can be observed the function syntax is very straight forward, and to examine the efficiency of it, we use the `@btime` macro from the BenchmarkTools package,

```julia
julia> using BenchmarkTools
julia> @btime byrow(ds, sum, 1:100);
  7.874 ms (2143 allocations: 1.02 MiB)
julia> m = Matrix(ds)
julia> @btime sum(m, dims = 2)
  20.773 ms (7 allocations: 879.11 KiB)
```

When the data set has heterogeneous data types at each column, `byrow` will be even more efficient than `sum(m, dims = 2)`.

### Optimised `fun`

Generally, `byrow` is very efficient for any `fun` which returns a single value for each row, however, it is fine tuned for the following functions:

* `all`
* `any`
* `count`
* `hash`
* `maximum`
* `mean`
* `minimum`
* `nunique`
* `prod`
* `std`
* `sum`
* `var`

The common syntax of `byrow` for all of these function except `nunique` is:

`byrow(ds, fun, cols; [by , threads = true])`

The `by` keyword argument is for giving a function to call on each value before calling `fun` to aggregate the values, and `threads = true` causes `byrow` to use all cores available to Julia for performing the computations. 
 
The `nunique` function doesn't accept `threads` argument, however, it has an extra keyword argument `count_missing`. `nunique` counts the number of unique value of each row, and `count_missing = true` counts missings as a unique value. 

#### Examples

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

To compute the mean of each row for the float variables, we simply call, 

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

To calculate the mean of the absolute value of each row for the float variables we use the same code and add `by = abs` as the keyword argument,

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

To find rows which contains at least one missing value in any of the column we can use the following code, 

```jldoctest
julia> byrow(ds, any, :, by = ismissing)
5-element Vector{Bool}:
 1
 1
 0
 1
 1
```

It means that except the third row, all other rows contains missing values. Using `byrow` with `count` function, we can count the number of non-missing values in each row, 

```jldoctest
julia> byrow(ds, count, :, by = !ismissing)
5-element Vector{Int32}:
 4
 3
 6
 4
 5
```

### General `fun`

For user defined functions which return a single value, `byrow` treats each row as a vector of values, thus the user defined function must accept a vector and return a single value. For instance to calculate `1 * col1 + 2 * col2 + 3 * col3` for each row in `ds` we can define the following function:

```jldoctest
julia> avg(x) = 1 * x[1] + 2 * x[2] + 3 * x[3]
avg (generic function with 1 method)
```

and use this function in `byrow`, 

```jldoctest
julia> byrow(ds, avg, 1:3)
5-element Vector{Union{Missing, Int64}}:
 10
  7
  6
   missing
  0
```

Note that `avg` is missing if any of the values of `x` is missing.

### Special operations

`byrow` also support a few optimised operations which return a vector of values for each row. The `fun` argument for these operations are:

* `cumprod`
* `cumprod!`
* `cumsum`
* `cumsum!`
* `sort`
* `sort!`
* `stdze`
* `stdze!`

The main difference between these operations and the previous operations is that these operations return a data set with the corresponding row has been updated with the operation. For the operations with `!` the updated version of the original data set is returned and for the operations without `!` a modified copy of the original data set is returned.

`cumsum` and `cumprod` calculate the cumulative sum and cumulative product of rows, respectively, `sort` sorts the values in each row, and `stdze` standardises the values in each row. The `sort` operation accepts all keyword arguments that the function `sort` in Julia Base accept.

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

Note that `cumsum` treats `missing` as zero, and `cumprod` treats `missing` as one.

The special operations don't change the variable names or the order of columns.