# Group observations

## Introduction

InMemoryDatasets uses two approaches to group observations: sorting, and hashing. In sorting approach, it sorts the data set based on given columns and finds the starts and ends of each group based on the sorted values. In hashing approach, it uses a customised algorithm to group observations. Each of these approaches has some advantages over the other one and for any particular problem one of them might be more suitable than the other one.

## `groupby!` and `groupby`

The main functions for grouping observations based on sorting approach are `groupby!` and `groupby`. The `groupby!` function replaces the original data set with the sorted one and attaches a meta information about the grouping orders to the replaced data set, on the other hand, the `groupby` function performs the sorting phase, however, it creates a view of the main data set where the meta information is attached to it. The output of `groupby` is basically a view of the sorted data set.

The syntax for calling `groupby!` and `groupby` is the same as the `sort!` function. This means `groupby!` and `groupby` accept all keyword arguments that the `sort!` function supports, these include:

* `rev` with default value of `false`
* `mapformats` with default value of `true`, i.e. by default these functions group data sets based on the formatted values.
* `stable` with default value of `true`
* `alg` which by default is set to `HeapSortAlg`, and it can be set as `QuickSort` too.

> Removing formats of columns that are used for `groupby!` with `mapformats = true` removes the grouping information too, i.e. the data set will not be marked as grouped/sorted data set .

### Examples

```jldoctest
julia> ds = Dataset(g = [1, 2, 1, 2, 1, 2], x = [12.0, 12.3, 11.0, 13.0, 15.0, 13.2])
6×2 Dataset
 Row │ g         x        
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1      12.0
   2 │        2      12.3
   3 │        1      11.0
   4 │        2      13.0
   5 │        1      15.0
   6 │        2      13.2

julia> groupby!(ds, 1)
6×2 Grouped Dataset with 2 groups
Grouped by: g
 Row │ g         x        
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1      12.0
   2 │        1      11.0
   3 │        1      15.0
   4 │        2      12.3
   5 │        2      13.0
   6 │        2      13.2

julia> ds # ds has been replaced with its grouped version
6×2 Grouped Dataset with 2 groups
Grouped by: g
 Row │ g         x        
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1      12.0
   2 │        1      11.0
   3 │        1      15.0
   4 │        2      12.3
   5 │        2      13.0
   6 │        2      13.2

julia> ds = Dataset(group = ["c1", "c2", "c1", "c3", "c1", "c3"], x = 1:6)
6×2 Dataset
 Row │ group     x        
     │ identity  identity
     │ String?   Int64?   
─────┼────────────────────
   1 │ c1               1
   2 │ c2               2
   3 │ c1               3
   4 │ c3               4
   5 │ c1               5
   6 │ c3               6

julia> groupby(ds, :group)
6×2 View of Grouped Dataset, Grouped by: group
 group     x        
 identity  identity
 String?   Int64?   
────────────────────
 c1               1
 c1               3
 c1               5
 c2               2
 c3               4
 c3               6

julia> ds # ds is untouched
6×2 Dataset
 Row │ group     x        
     │ identity  identity
     │ String?   Int64?   
─────┼────────────────────
   1 │ c1               1
   2 │ c2               2
   3 │ c1               3
   4 │ c3               4
   5 │ c1               5
   6 │ c3               6
julia> salary = Dataset(id = 1:10,
                       salary=[100, 120, 301, 95, 200, 75, 150, 67, 90, 110])
10×2 Dataset
 Row │ id        salary   
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1       100
   2 │        2       120
   3 │        3       301
   4 │        4        95
   5 │        5       200
   6 │        6        75
   7 │        7       150
   8 │        8        67
   9 │        9        90
  10 │       10       110

julia> s_grp(x) = x < 100 ? 1 : x < 200 ? 2 : 3
s_grp (generic function with 1 method)

julia> setformat!(salary, :salary => s_grp)
10×2 Dataset
 Row │ id        salary
     │ identity  s_grp  
     │ Int64?    Int64?
─────┼──────────────────
   1 │        1       2
   2 │        2       2
   3 │        3       3
   4 │        4       1
   5 │        5       3
   6 │        6       1
   7 │        7       2
   8 │        8       1
   9 │        9       1
  10 │       10       2

julia> groupby(salary, 2)
10×2 View of Grouped Dataset, Grouped by: salary
 id        salary
 identity  s_grp  
 Int64?    Int64?
──────────────────
        4       1
        6       1
        8       1
        9       1
        1       2
        2       2
        7       2
       10       2
        3       3
        5       3
```

The `groupby!` and `groupby` functions accept the output of the `groupby` function. Thus, some may use these functions to incrementally group a data set.

When the `groupby!` function is used on a data set, the data set is marked as a grouped data set and the functions which handle grouped data set differently are signalled when the grouped data sets are passed as their arguments. Two of those functions are `modify!` and `modify` functions. When a grouped data set is passed to these two functions, InMemoryDatasets applies each modification within each group. The `modify!` and `modify` functions treat the view of a grouped data set (produced by the `groupby` function) in the same way without changing the order of the original data set. For better performance, set `stable = false` when the `groupby` function is used in conjunction with `modify!` or `modify`.

### Examples

```jldoctest
julia> ds = Dataset(g = [2, 1, 1, 2, 2],
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
   1 │        2         0         3        1.2  missing    missing   
   2 │        1         0         2  missing    missing    missing   
   3 │        1         1         1       -1.0        3.0       -1.4
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0

julia> groupby!(ds, 1)
5×6 Grouped Dataset with 2 groups
Grouped by: g
 Row │ g         x1_int    x2_int    x1_float   x2_float   x3_float  
     │ identity  identity  identity  identity   identity   identity  
     │ Int64?    Int64?    Int64?    Float64?   Float64?   Float64?  
─────┼───────────────────────────────────────────────────────────────
   1 │        1         0         2  missing    missing    missing   
   2 │        1         1         1       -1.0        3.0       -1.4
   3 │        2         0         3        1.2  missing    missing   
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0

julia> modify(ds, r"int" => x -> x .- maximum(x))
5×6 Grouped Dataset with 2 groups
Grouped by: g
 Row │ g         x1_int    x2_int    x1_float   x2_float   x3_float  
     │ identity  identity  identity  identity   identity   identity  
     │ Int64?    Int64?    Int64?    Float64?   Float64?   Float64?  
─────┼───────────────────────────────────────────────────────────────
   1 │        1        -1         0  missing    missing    missing   
   2 │        1         0        -1       -1.0        3.0       -1.4
   3 │        2        -2         0        1.2  missing    missing   
   4 │        2   missing         0        2.3  missing          3.0
   5 │        2         0        -5       10.0  missing       -100.0

julia> sale = Dataset(date = [Date(2012, 11), Date(2013, 5), Date(2012, 4),
                                     Date(2013, 1), Date(2014, 8), Date(2013, 2)],
                             sale = [100, 200, 140, 200, 132, 150])
6×2 Dataset
 Row │ date        sale     
     │ identity    identity
     │ Date?       Int64?   
─────┼──────────────────────
   1 │ 2012-11-01       100
   2 │ 2013-05-01       200
   3 │ 2012-04-01       140
   4 │ 2013-01-01       200
   5 │ 2014-08-01       132
   6 │ 2013-02-01       150

julia> setformat!(sale, :date=>year)
6×2 Dataset
 Row │ date   sale     
     │ year   identity
     │ Date?  Int64?   
─────┼─────────────────
   1 │ 2012        100
   2 │ 2013        200
   3 │ 2012        140
   4 │ 2013        200
   5 │ 2014        132
   6 │ 2013        150

julia> spct(x) = x ./ sum(x) .* 100
spct (generic function with 1 method)

julia> modify(groupby(sale, :date), :sale => spct => :sale_pct)
6×3 Dataset
 Row │ date   sale      sale_pct
     │ year   identity  identity
     │ Date?  Int64?    Float64?
─────┼───────────────────────────
   1 │ 2012        100   41.6667
   2 │ 2013        200   36.3636
   3 │ 2012        140   58.3333
   4 │ 2013        200   36.3636
   5 │ 2014        132  100.0
   6 │ 2013        150   27.2727
```

## `ungroup!`

The `ungroup!` function is a utility function that removes the `grouped` mark from a grouped data set produced by `groupby!`. The function doesn't change the permutation of the data set, thus, even the data set is not any more grouped, it is still sorted, and it is very efficient to re-group it. However, note that the last modified time of the data set is updated when `ungroup!` is called on a data set.

The `ungroup!` function can be used in scenarios that one needs to modify a data set but it is not desired to apply a specific modification within each group, instead the modification is needed to be applied to the whole column. In these kind of situations, first `ungroup!` is used to remove the grouping mark and then the `modify!` function can be used on the data set. The `groupby!` function can be used afterward to mark the data set as grouped data set.

## `gatherby`

The `gatherby` function uses the hashing approach to group observations based on a set of columns. InMemoryDatasets uses a customised algorithm to gather observations which sometimes does this without using the `hash` function. The `gatherby` function doesn't sort the data set, instead, it uses the in-house developed algorithm to group observations. `gatherby` can be particularly useful when sorting is computationally expensive. Another benefit of `gatherby` is that, by default, it keeps the order of observations in each group the same as their appearance in the original data set.

The `gatherby` function uses the formatted values for gathering the observations into groups, however, using `mapformats = false` changes this behaviour.

The syntax for using the `gatherby` function is `gatherby(ds, cols)` where `ds` is the data set and `cols` is any column selector which indicates the columns which are going to be used in gathering.

### Examples

```jldoctest
julia> ds = Dataset(grp = [1, 2, 3, 3, 1, 3, 2, 1],
                  x = [true, false, true, true, true, true, false, false])
8×2 Dataset
 Row │ grp       x        
     │ identity  identity
     │ Int64?    Bool?    
─────┼────────────────────
   1 │        1      true
   2 │        2     false
   3 │        3      true
   4 │        3      true
   5 │        1      true
   6 │        3      true
   7 │        2     false
   8 │        1     false

julia> gatherby(ds, :x)
8×2 View of GatherBy Dataset, Gathered by: x
 grp       x        
 identity  identity
 Int64?    Bool?    
────────────────────
        1      true
        3      true
        3      true
        1      true
        3      true
        2     false
        2     false
        1     false

julia> setformat!(ds, 1=>isodd)
8×2 Dataset
 Row │ grp     x        
     │ isodd   identity
     │ Int64?  Bool?    
─────┼──────────────────
   1 │   true      true
   2 │  false     false
   3 │   true      true
   4 │   true      true
   5 │   true      true
   6 │   true      true
   7 │  false     false
   8 │   true     false

julia> gatherby(ds, 1)
8×2 View of GatherBy Dataset, Gathered by: grp
 grp     x        
 isodd   identity
 Int64?  Bool?    
──────────────────
   true      true
   true      true
   true      true
   true      true
   true      true
   true     false
  false     false
  false     false
```

Similar to `groupby!/groupby` functions, `gatherby` can be passed to functions which operate on grouped data sets.

As mentioned before, the result of `gatherby` is stable, i.e. the observations order within each group will be the order of their appearance in the original data set. However, when this stability is not needed and there are many groups in the data set, passing `stable = false` improves the performance by sacrificing the stability.

The `gatherby` function has one extra keyword arguments, `isgathered`, which by default is set to `false`. When this argument is set to `true`, InMemoryDatasets assumes that the observations are currently gathered by some rules and it only finds the starts and ends of each group and marks the data set as gathered. So users can manually group observations by setting this keyword argument.
