# Sorting Datasets

## Introduction

Sorting is one of the key tasks for Datasets. Actually, when we group a data set by given set of columns, InMemoryDatasets does sorting behind the scene and groups the observations based on their sorted values. The joining algorithms also uses the sorting functions for finding the matched observations. One may sort a data set based on a set of columns by either their formatted values, or their actual values. In this section we go through the main functions for sorting Datasets.

> Note that InMemoryDatasets uses parallel algorithms for sorting observations.

## `sort!/sort`

The `sort!` function accepts a Dataset and a set of columns and sorts the given Dataset based on provided columns. By default the `sort!` function does the sorting based on the formatted values, however, using `mapformats = false` forces the sorting be done based on the actual values. `sort!` doesn't create a new dataset, it only replaces the original one with the sorted one. If the original data set needed to be untouched the `sort` function must be used. By default, both `sort!` and `sort` functions do a stable sort using a `Heap` sort algorithm. If the stability of the sort is not needed, using the keyword option `stable = false` can improve the performance. User can also change the default sorting algorithm to `QuickSort` by using the `alg = QuickSort` option. By default the ascending sorting is used for the sorting task, and using `rev = true` changes it to descending ordering, and for multiple columns a vector of  `true`, `false` can be supplied for this option, i.e. each column can be sorted in ascending or descending order independently. Note that:

* Datasets uses `isless` for checking the order of values.

* Datasets prints extra information when it shows a sorted data set.

* Like Julia Base, the missing values are treated larger than any other values.

* `sort` creates a copy of data and permutes each column of it and attaches some attributes to the new data set. To sort a data set without creating a new data set or modifying the original data set, someone may use the `groupby` function. The `groupby` function sorts and then creates a meta information about the sorted data set. The `groupby` function will be discussed in another section in detail.

### Examples

```jldoctest
julia> ds = Dataset(x = [5,4,3,2,1], y = [42,52,4,1,55])
5×2 Dataset
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        5        42
   2 │        4        52
   3 │        3         4
   4 │        2         1
   5 │        1        55

julia> sort!(ds, :x);
julia> ds
5×2 Sorted Dataset
 Sorted by: x
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1        55
   2 │        2         1
   3 │        3         4
   4 │        4        52
   5 │        5        42

julia> sort(ds, :y, rev = true)
5×2 Sorted Dataset
 Sorted by: y
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1        55
   2 │        4        52
   3 │        5        42
   4 │        3         4
   5 │        2         1

julia> ds = Dataset(x = [5, 4, missing, 4],
                    y = [3, missing, missing , 1])
4×2 Dataset
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        5         3
   2 │        4   missing
   3 │  missing   missing
   4 │        4         1

julia> sort(ds, 1:2)
4×2 Sorted Dataset
 Sorted by: x, y
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        4         1
   2 │        4   missing
   3 │        5         3
   4 │  missing   missing

julia> sort(ds, 1:2, rev = [false, true])
4×2 Sorted Dataset
 Sorted by: x, y
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        4   missing
   2 │        4         1
   3 │        5         3
   4 │  missing   missing
```

The following examples show how the sorting functions work with formats.

```jldoctest
julia> ds = Dataset(state = ["CA", "TX", "IL", "IL", "IL", "CA", "TX", "TX"],
                    date = [Date("2020-01-01"), Date("2020-03-01"), Date("2020-01-01"),
                            Date("2020-03-01"), Date("2020-02-01"), Date("2021-03-01"),
                            Date("2021-02-01"), Date("2020-02-01")],
                      qt = [123, 143, 144, 199, 153, 144, 134, 188])
8×3 Dataset
 Row │ state     date        qt       
     │ identity  identity    identity
     │ String?   Date?       Int64?   
─────┼────────────────────────────────
   1 │ CA        2020-01-01       123
   2 │ TX        2020-03-01       143
   3 │ IL        2020-01-01       144
   4 │ IL        2020-03-01       199
   5 │ IL        2020-02-01       153
   6 │ CA        2021-03-01       144
   7 │ TX        2021-02-01       134
   8 │ TX        2020-02-01       188

julia> setformat!(ds, :date=>month)
8×3 Dataset
 Row │ state     date   qt       
     │ identity  month  identity
     │ String?   Date?  Int64?   
─────┼───────────────────────────
   1 │ CA        1           123
   2 │ TX        3           143
   3 │ IL        1           144
   4 │ IL        3           199
   5 │ IL        2           153
   6 │ CA        3           144
   7 │ TX        2           134
   8 │ TX        2           188

julia> sort(ds, [2,1])
8×3 Sorted Dataset
 Sorted by: date, state
 Row │ state     date   qt       
     │ identity  month  identity
     │ String?   Date?  Int64?   
─────┼───────────────────────────
   1 │ CA        1           123
   2 │ IL        1           144
   3 │ IL        2           153
   4 │ TX        2           134
   5 │ TX        2           188
   6 │ CA        3           144
   7 │ IL        3           199
   8 │ TX        3           143

julia> sort(ds, [2,1], mapformats = false)
8×3 Sorted Dataset
 Sorted by: date, state
 Row │ state     date   qt       
     │ identity  month  identity
     │ String?   Date?  Int64?   
─────┼───────────────────────────
   1 │ CA        1           123
   2 │ IL        1           144
   3 │ IL        2           153
   4 │ TX        2           188
   5 │ IL        3           199
   6 │ TX        3           143
   7 │ TX        2           134
   8 │ CA        3           144

julia> sort(ds, [1,2], mapformats = false)
8×3 Sorted Dataset
 Sorted by: state, date
 Row │ state     date   qt       
     │ identity  month  identity
     │ String?   Date?  Int64?   
─────┼───────────────────────────
   1 │ CA        1           123
   2 │ CA        3           144
   3 │ IL        1           144
   4 │ IL        2           153
   5 │ IL        3           199
   6 │ TX        2           188
   7 │ TX        3           143
   8 │ TX        2           134
```

In some scenarios the performance of sort can be improved by using formats. For example, when we know for a specific column there is only a few numbers after the decimal point, using a format can improve the performance of the sort. In the following example we are using the `@btime` macro from the `BenchmarkTools` package to demonstrate this;

```jldoctest
# column :x1 has at most 2 digits after the decimal point
julia> ds = Dataset(x1 = round.(rand(10^6),digits = 2),
               x2 = repeat(1:100, 10^4));
julia> @btime sort(ds, 1);
  56.278 ms (1661 allocations: 67.65 MiB)

julia> custom_fmt(x) = round(x * 100);
julia> setformat!(ds, 1=>custom_fmt);
julia> @btime sort(ds, 1);
  13.718 ms (446 allocations: 53.44 MiB)
```

The 4 times improvement in the performance is due to the fact that the formatted values in the data set are basically integer rather than float (the actual values) and the algorithms for sorting integers are usually faster than those for sorting double precision numbers.

Another trick can be used for situations when a data set contains a column of string values where the values can be treated as numbers, e.g. in the following code `:x1` is basically integer values with `"id"` been attached to each value, here we use a customised format that extracts the numeric values from `:x1`;

```jldoctest
julia> ds = Dataset(x1 = "id" .* string.(rand(1:100000, 10^6)));
julia> @btime sort(ds, 1);
  296.101 ms (612 allocations: 54.40 MiB)
julia> custom_fmt(x) = parse(Int, @views x[3:end])
   custom_fmt (generic function with 1 method)
julia> setformat!(ds, 1=>custom_fmt);
julia> @btime sort(ds, 1)
  38.057 ms (323 allocations: 44.27 MiB)
```

## `sortperm`

The `sortperm(ds, cols)` function returns a permutation vector `perm` that puts `ds[perm, :]` in sorted order based on `cols`. Similar to `sort!`/`sort`, this function accepts `rev`, `alg`, `mapformats` and `stable` options.

### Examples

```jldoctest
julia> ds = Dataset(x = [1,4,3,2,1], y = [420,52,4,1,55])
5×2 Dataset
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1       420
   2 │        4        52
   3 │        3         4
   4 │        2         1
   5 │        1        55

julia> p = sortperm(ds, [1,2])
5-element Vector{Int32}:
5
1
4
3
2

julia> ds[p, :]
5×2 Dataset
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1        55
   2 │        1       420
   3 │        2         1
   4 │        3         4
   5 │        4        52
```

## `unsort!`

The `unsort!` function undo the last sort operation that has been done on a data set, i.e. when a `sort!` function has been applied to a data set, directly or indirectly (e.g. the `groupby!` function is one of the functions which uses `sort!` behind the scene), the `unsort!` function can undo it.

```jldoctest
julia> ds = Dataset(x = [1,3,1,2,6,1,4,4],
                    y = [100,150,90,110,100,80,50,30])
8×2 Dataset
Row │ x         y        
    │ identity  identity
    │ Int64?    Int64?   
────┼────────────────────
  1 │        1       100
  2 │        3       150
  3 │        1        90
  4 │        2       110
  5 │        6       100
  6 │        1        80
  7 │        4        50
  8 │        4        30

julia> sort!(ds, 1:2);
julia> ds
8×2 Sorted Dataset
 Sorted by: x, y
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1        80
   2 │        1        90
   3 │        1       100
   4 │        2       110
   5 │        3       150
   6 │        4        30
   7 │        4        50
   8 │        6       100

julia> unsort!(ds)
8×2 Dataset
 Row │ x         y        
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1       100
   2 │        3       150
   3 │        1        90
   4 │        2       110
   5 │        6       100
   6 │        1        80
   7 │        4        50
   8 │        4        30
```

## `issorted`/`issorted!`
