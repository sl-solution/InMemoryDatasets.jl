# Aggregation

## Introduction

In previous sections we discuss the main functions (`groupby!`, `groupby`, and `gatherby`) to group observations in a data set. In this section we introduce a function which can be used to apply a function on each group of observations.

## `combine`

`combine` is a function which accepts a grouped data set - created by `groupby!`, `groupby`, or `gatherby` - and a set of operations in the form of `cols => fun`, where `cols` is a column selector, and applies `fun` on each columns in `cols`. The operations can be passed as `col => fun => :newname` if user likes to have a specific column name for the output column. All columns selected by `cols` are assumed to be referring to the same columns in the input data set. However, when the passed `fun` is a `byrow` function then `combine` assumes every column in `cols` are referring to the existing columns in the output data set. Thus, unlike `modify!`/`modify`, `combine` only can access to the existing columns in the output data set when the `fun` is a `byrow` function.

The order of the output data set depends on the passed data set, i.e. for `groupby` the order of the output is sorted order of the grouping columns, and for `gatherby` data set the order of the output is based on the appearance of observations in the original data set. Since for most situations, the stability of grouping is not needed, passing `stable = false` in `groupby/gatherby` can improve the performance, but when `stable = false` for `gatherby`, the order of the output is undefined.

By default, `combine` outputs the grouping columns in the final result, however, passing `dropgroupcols = true` removes them from the final output.

> By default `combine` send each group of data to different threads for processing (multithreaded processing), however, passing `threads = false` changes this.

### Examples

```jldoctest
julia> ds = Dataset(g = [1,2,1,2,1,2], x = 1:6)
6×2 Dataset
 Row │ g         x        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        1         3
   4 │        2         4
   5 │        1         5
   6 │        2         6

julia> combine(groupby(ds, :g), :x=>[IMD.sum, mean])
2×3 Dataset
 Row │ g         sum_x     mean_x   
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1         9       3.0
   2 │        2        12       4.0

julia> combine(gatherby(ds, :g), :x => [IMD.maximum, IMD.minimum], 2:3 => byrow(-) => :range)
2×4 Dataset
 Row │ g         maximum_x  minimum_x  range    
     │ identity  identity   identity   identity
     │ Int64?    Int64?     Int64?     Int64?   
─────┼──────────────────────────────────────────
   1 │        1          5          1         4
   2 │        2          6          2         4

julia> ds = Dataset(rand(1:10, 10, 4), :auto)
10×4 Dataset
 Row │ x1        x2        x3        x4       
     │ identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────
   1 │        9         1         6         3
   2 │       10         7        10         6
   3 │        7         7         3         9
   4 │        9         4        10         8
   5 │        7         3         4         5
   6 │        2         6         5         6
   7 │        1         6         6         1
   8 │       10         2         7         6
   9 │        5        10         9         6
  10 │        1         1         3         4

julia> combine(gatherby(ds, 1), r"x" => IMD.sum)
6×5 Dataset
 Row │ x1        sum_x1    sum_x2    sum_x3    sum_x4   
     │ identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼──────────────────────────────────────────────────
   1 │        9        18         5        16        11
   2 │       10        20         9        17        12
   3 │        7        14        10         7        14
   4 │        2         2         6         5         6
   5 │        1         2         7         9         5
   6 │        5         5        10         9         6

julia> ds = Dataset(g = [1,2,1,2,1,2], x = 1:6)
6×2 Dataset
 Row │ g         x        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        1         3
   4 │        2         4
   5 │        1         5
   6 │        2         6

julia> combine(gatherby(ds, :g), :x=>[IMD.maximum, IMD.minimum], 2:3=>byrow(-)=>:range, dropgroupcols = true)
2×3 Dataset
 Row │ maximum_x  minimum_x  range    
     │ identity   identity   identity
     │ Int64?     Int64?     Int64?   
─────┼────────────────────────────────
   1 │         5          1         4
   2 │         6          2         4
```

`combine` treats each columns in `cols` individually, thus, a function can be applied to each column by `cols => fun` form. `combine` normalises `cols => funs` to `col1 => funs`, `col2 => funs`, ..., where `col1` refers to the first column in the column selector `cols`, `col2` refers to the second one, .... When `col => funs` is passed to the function where `col` refers to a single column, `combine` normalises it as `col => fun1`, `col => fun2`, ..., where `fun1`, `fun2`,... are the first, second, ... functions in passed `funs`.

Any reduction on multiple columns should be go through a `byrow` approach.

In special cases, where users like to apply a multivariate function on a set of columns, the columns which are going to be the argument of the multivariate function must be passed as a Tuple of column names or column indices.

```jldoctest
julia> ds = Dataset(g = [1,1,1,2,2,2],
            x1 = [1.2, 2.3, 1.3, 2.4, 4.5, 5.1],
            x2 = [11, 12.0, 11.0, 12.3, 14.5, 16.9])
6×3 Dataset
Row │ g         x1        x2       
    │ identity  identity  identity
    │ Int64?    Float64?  Float64?
────┼──────────────────────────────
  1 │        1       1.2      11.0
  2 │        1       2.3      12.0
  3 │        1       1.3      11.0
  4 │        2       2.4      12.3
  5 │        2       4.5      14.5
  6 │        2       5.1      16.9

julia> combine(gatherby(ds, 1, isgathered=true), (2,3)=>cor)
2×2 Dataset
Row │ g         cor_x1_x2
    │ identity  identity  
    │ Int64?    Float64?  
────┼─────────────────────
  1 │        1   0.996616
  2 │        2   0.944252

```
