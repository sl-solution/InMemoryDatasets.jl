# Filter observations

## Introduction

In this section, the Datasets' APIs for filtering observations are discussed. We provides information about
three main ways to filter observations based on some conditions, 1) using the `byrow` function, 2) using the `mask` function, 3) and using Julia broadcasting.

## `byrow`

`byrow` has been discussed previously in details. However, in this section we are going to use it for
filtering observations. To use `byrow(ds, fun, cols, ...)` for filtering observations, the `fun` argument should
be set as `all` or `any`, and supply the conditions by using the `by` keyword option. The supplied `by` will be checked for each observation in all selected columns. The function returns a boolean vector where its `j`th elements will be equivalent to the result of `all(by, [col1[j], col2[j], ...])` or `any(by, [col1[j], col2[j], ...])` when `all` or `any` is set as the `fun` argument, respectively.

The main feature of `byrow(ds, fun, cols, by = ...)` when `fun` is `all/any` is that the `by` keyword argument can be a vector of functions. Thus, when a multiple columns are supplied as `cols` each column can have its own `by`.

### `filter` and `filter!`

The `filter` and `filter!` functions are two shortcuts for doing the `byrow` and `getindex` operations at the same call.

`filter(ds, cols; [type = all, by = isequal(true),...])` is the shortcut for `ds[byrow(ds, type, cols; by = by,...), :]`, and `filter!(ds, cols; [type = all, by = isequal(true),...])` is the shortcut for `deleteat![ds, byrow(ds, type, cols; by = by,...))`.

### Examples

The first expression creates a data set, and in the second one we use `byrow` to filter `all` rows which the values of all columns are equal to 1.

```jldoctest
julia> ds = Dataset(x1 = 1, x2 = 1:10, x3 = repeat(1:2, 5))
10×3 Dataset
 Row │ x1        x2        x3       
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │        1         1         1
   2 │        1         2         2
   3 │        1         3         1
   4 │        1         4         2
   5 │        1         5         1
   6 │        1         6         2
   7 │        1         7         1
   8 │        1         8         2
   9 │        1         9         1
  10 │        1        10         2

julia> byrow(ds, all, :, by = isequal(1))
10-element Vector{Bool}:
1
0
0
0
0
0
0
0
0
0
```

Note that only the first row is meeting the condition. As another example, let's see the code which
filter all rows which the numbers in all columns are odd.

```jldoctest
julia> filter(ds, :, by = isodd)

 5×3 Dataset
  Row │ x1        x2        x3       
      │ identity  identity  identity
      │ Int64?    Int64?    Int64?   
 ─────┼──────────────────────────────
    1 │        1         1         1
    2 │        1         3         1
    3 │        1         5         1
    4 │        1         7         1
    5 │        1         9         1
```

In the next example we are going to filter all rows which the value of any of column is greater than 5.

```jldoctest
julia> byrow(ds, any, :, by = >(5))
10-element Vector{Bool}:
 0
 0
 0
 0
 0
 1
 1
 1
 1
 1
```

The next example shows how a vector of functions can be supplied:

```jldoctest
julia> byrow(ds, all, 2:3, by = [>(5), isodd])
10-element Vector{Bool}:
 0
 0
 0
 0
 0
 0
 1
 0
 1
 0
```

We can use the combination of `modify!/modify` and `byrow` to filter observations based on all values in a column, e.g. in the following example we filter all rows which `:x2` and `:x3` are larger than their means:

```jldoctest
julia> modify!(ds, 2:3 .=> (x -> x .> mean(x)) .=> [:_tmp1, :_tmp2])
10×5 Dataset
 Row │ x1        x2        x3        _tmp1     _tmp2    
     │ identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Bool?     Bool?    
─────┼──────────────────────────────────────────────────
   1 │        1         1         1     false     false
   2 │        1         2         2     false      true
   3 │        1         3         1     false     false
   4 │        1         4         2     false      true
   5 │        1         5         1     false     false
   6 │        1         6         2      true      true
   7 │        1         7         1      true     false
   8 │        1         8         2      true      true
   9 │        1         9         1      true     false
  10 │        1        10         2      true      true

julia> filter(ds, r"_tm") # translate to ds[byrow(ds, all, r"_tm"), :]

3×5 Dataset
Row │ x1        x2        x3        _tmp1     _tmp2    
    │ identity  identity  identity  identity  identity
    │ Int64?    Int64?    Int64?    Bool?     Bool?    
────┼──────────────────────────────────────────────────
  1 │        1         6         2      true      true
  2 │        1         8         2      true      true
  3 │        1        10         2      true      true
```

> Note that to drop the temporary columns we can use the `select!` function.

## `mask`

`mask` is a function which calls a function (or a vector of functions) on all observations of a set of selected columns. The syntax for `mask` is very similar to `map` function:

> `mask(ds, funs, cols, [mapformats = true, missings = false, threads = true])`

however, unlike `map`, the function doesn't return the whole modified dataset, it returns a boolean data set with the same number of rows as `ds` and the same number of columns as the length of `cols`, while `fun` has been called on each observation. The return value of `fun` must be `true`, `false`, or `missing`. The combination of `mask` and `byrow` can be used to filter observations.

 Compared to `byrow`, the `mask` function has some useful features which are handy in some scenarios:

* `mask` returns a boolean data set which shows exactly which observation will be selected when `fun` is called on it.
* By default, the `mask` function filters observations based on their formatted values. However, this can be changed by setting `mapformats = false`.
* By default, the `mask` function will treat the missing values as `false`, however, this behaviour can be modified by using the keyword option `missings`. This option can be set as `true`, `false`(default value), or `missing`.

### Examples

```jldoctest
julia> ds = Dataset(x1 = repeat(1:2, 5), x2 = 1:10, x3 = repeat([missing, 2], 5))
10×3 Dataset
 Row │ x1        x2        x3       
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │        1         1   missing
   2 │        2         2         2
   3 │        1         3   missing
   4 │        2         4         2
   5 │        1         5   missing
   6 │        2         6         2
   7 │        1         7   missing
   8 │        2         8         2
   9 │        1         9   missing
  10 │        2        10         2

julia> setformat!(ds, 2 => isodd)
10×3 Dataset
Row │ x1        x2      x3       
    │ identity  isodd   identity
    │ Int64?    Int64?  Int64?   
────┼────────────────────────────
  1 │        1    true   missing
  2 │        2   false         2
  3 │        1    true   missing
  4 │        2   false         2
  5 │        1    true   missing
  6 │        2   false         2
  7 │        1    true   missing
  8 │        2   false         2
  9 │        1    true   missing
 10 │        2   false         2

julia>  mask(ds, isequal(1), :) # simple use case
10×3 Dataset
 Row │ x1        x2        x3       
     │ identity  identity  identity
     │ Bool?     Bool?     Bool?    
─────┼──────────────────────────────
   1 │     true      true     false
   2 │    false     false     false
   3 │     true      true     false
   4 │    false     false     false
   5 │     true      true     false
   6 │    false     false     false
   7 │     true      true     false
   8 │    false     false     false
   9 │     true      true     false
  10 │    false     false     false

julia> _tmp = mask(ds, isequal(1), :, mapformats = false) # use the actual values instead of formatted values
10×3 Dataset
Row │ x1        x2        x3       
    │ identity  identity  identity
    │ Bool?     Bool?     Bool?    
────┼──────────────────────────────
  1 │     true      true     false
  2 │    false     false     false
  3 │     true     false     false
  4 │    false     false     false
  5 │     true     false     false
  6 │    false     false     false
  7 │     true     false     false
  8 │    false     false     false
  9 │     true     false     false
 10 │    false     false     false

julia> filter(_tmp, :, type = any) # OR ds[byrow(_tmp, any, :), :]. This uses the result of previous run
5×3 Dataset
 Row │ x1        x2      x3       
     │ identity  isodd   identity
     │ Int64?    Int64?  Int64?   
─────┼────────────────────────────
   1 │        1    true   missing
   2 │        1    true   missing
   3 │        1    true   missing
   4 │        1    true   missing
   5 │        1    true   missing

julia> mask(ds, [isodd, ==(2)], 2:3, missings = missing) # using a vector of functions and setting missings option
10×2 Dataset
 Row │ x2        x3       
     │ identity  identity
     │ Bool?     Bool?    
─────┼────────────────────
   1 │     true   missing
   2 │    false      true
   3 │     true   missing
   4 │    false      true
   5 │     true   missing
   6 │    false      true
   7 │     true   missing
   8 │    false      true
   9 │     true   missing
  10 │    false      true
```

## Julia broadcasting

For simple use case (e.g. when working on a single column) we can use broadcasting directly. For example if we are interested on rows which the first column is greater than 5 we can directly use (assume the data set is called `ds`):

> `ds[ds[!, 1] .> 1, :]`

or use the column names.

### Examples

In the following examples we use `.` for broadcasting, and its important to include it in your code when you are going to use this option for filtering observations.

```jldoctest
julia> ds = Dataset(x1 = repeat(1:2, 5), x2 = 1:10, x3 = repeat([missing, 2], 5))
10×3 Dataset
 Row │ x1        x2        x3       
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │        1         1   missing
   2 │        2         2         2
   3 │        1         3   missing
   4 │        2         4         2
   5 │        1         5   missing
   6 │        2         6         2
   7 │        1         7   missing
   8 │        2         8         2
   9 │        1         9   missing
  10 │        2        10         2

julia> ds[ds.x1 .== 2, :]
5×3 Dataset
Row │ x1        x2        x3       
    │ identity  identity  identity
    │ Int64?    Int64?    Int64?   
────┼──────────────────────────────
  1 │        2         2         2
  2 │        2         4         2
  3 │        2         6         2
  4 │        2         8         2
  5 │        2        10         2

julia> ds[(ds.x1 .== 1) .& (ds.x2 .> 5), :]
2×3 Dataset
Row │ x1        x2        x3       
    │ identity  identity  identity
    │ Int64?    Int64?    Int64?   
────┼──────────────────────────────
  1 │        1         7   missing
  2 │        1         9   missing
```
