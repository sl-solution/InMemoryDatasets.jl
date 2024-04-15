# InMemoryDatasets

[![](https://img.shields.io/badge/docs-stable-blue.svg)](https://sl-solution.github.io/InMemoryDatasets.jl/stable) [![CI](https://github.com/sl-solution/InMemoryDatasets.jl/actions/workflows/ci.yml/badge.svg)](https://github.com/sl-solution/InMemoryDatasets.jl/actions/workflows/ci.yml)

# Documentation

The latest release's Documentation is available via https://sl-solution.github.io/InMemoryDatasets.jl/stable.


# Introduction

`InMemoryDatasets.jl` is a multithreaded package for data manipulation and is designed for `Julia` 1.6+ (64bit OS). The core computation engine of the package is a set of customised algorithms developed specifically for columnar tables. The package performance is tuned with two goals in mind, a) low overhead of allowing missing values everywhere, and b) the following priorities - in order of importance:

1. Low compilation time
2. Memory efficiency
3. High performance

we do our best to keep the overall complexity of the package as low as possible to simplify:

* the maintenance of the package
* adding new features to the package
* contributing to the package

See [here](https://duckdblabs.github.io/db-benchmark/) for some benchmarks.
# Features

`InMemoryDatasets.jl` has many interesting features, here, we highlight some of our favourites (in no particular order):

* Assigning a named function to a column as its **format**
  * By default, formatted values are used for operations like: displaying, sorting, grouping, joining,...
  * Format evaluation is lazy
  * Formats don't change the actual values
* **Multi-threading** across the whole package
  * Most functions in `InMemoryDatasets.jl` exploit all cores available to `Julia` by default
  * Disabling parallel computation via passing the `threads = false` keyword argument to functions
* Powerful **row-wise** operations
  * Support many common operations
  * Specialised operations for modifying columns
  * Customised row-wise operations for **filtering** observations / `filter` simply wraps `byrow`
* Unique approach for **reshaping** data
  * **Unified** syntax for all type of reshaping
  * Cover all reshaping functions:
    * stacking and un-stacking on single/multiple columns
    * wide to long and long to wide reshaping
    * transposing and more
* Fast **sorting** algorithms
  * Stable and Unstable `HeapSort` and `QuickSort` algorithms
  * Count sort for integers
* Compiler friendly **grouping** algorithms
  * `groupby!`/`groupby` to group observation using sorting algorithms - sorted order
  * `gatherby` to group observation using hybrid hash algorithms - observations order
  * incremental grouping operation for
    `groupby!`/`groupby`, i.e. adding a column at a time
* Efficient **joining** algorithms
  * Preserve the order of observations in the left data set
  * Support two methods for joining: `sort-merge` join and `hash` join.
  * Customised columnar-hybrid-hash algorithms for join
  * Inequality-kind (**non-equi**) and **range joins** for `innerjoin`, `contains`, `semijoin!`/`semijoin`, `antijoin!`/`antijoin`
  * `closejoin!`/`closejoin` for **non exact match** join
  * `update!`/`update` for **updating** a master data set with values from a transaction data set

# Example

```julia
julia> using InMemoryDatasets
julia> g1 = repeat(1:6, inner = 4);
julia> g2 = repeat(1:4, 6);
julia> y = ["d8888b.  ", " .d8b.   ", "d888888b ", "  .d8b.  ", "88  `8D  ", "d8' `8b  ",
            "`~~88~~' ", " d8' `8b ", "88   88  ", "88ooo88  ", "   88    ", " 88ooo88 ",
            "88   88  ", "88~~~88  ", "   88    ", " 88~~~88 ", "88  .8D  ", "88   88  ",
            "   88    ", " 88   88 ", "Y8888D'  ", "YP   YP  ", "   YP    ", " YP   YP "];
julia> ds = Dataset(g1 = g1, g2 = g2, y = y)
24×3 Dataset
 Row │ g1        g2        y         
     │ identity  identity  identity  
     │ Int64?    Int64?    String?   
─────┼───────────────────────────────
   1 │        1         1  d8888b.
   2 │        1         2   .d8b.
   3 │        1         3  d888888b
   4 │        1         4    .d8b.
   5 │        2         1  88  `8D
   6 │        2         2  d8' `8b
   7 │        2         3  `~~88~~'
   8 │        2         4   d8' `8b
   9 │        3         1  88   88
  10 │        3         2  88ooo88
  11 │        3         3     88
  12 │        3         4   88ooo88
  13 │        4         1  88   88
  14 │        4         2  88~~~88
  15 │        4         3     88
  16 │        4         4   88~~~88
  17 │        5         1  88  .8D
  18 │        5         2  88   88
  19 │        5         3     88
  20 │        5         4   88   88
  21 │        6         1  Y8888D'
  22 │        6         2  YP   YP
  23 │        6         3     YP
  24 │        6         4   YP   YP

julia> sort(ds, :g2)
24×3 Sorted Dataset
 Sorted by: g2
 Row │ g1        g2        y         
     │ identity  identity  identity  
     │ Int64?    Int64?    String?   
─────┼───────────────────────────────
   1 │        1         1  d8888b.
   2 │        2         1  88  `8D
   3 │        3         1  88   88
   4 │        4         1  88   88
   5 │        5         1  88  .8D
   6 │        6         1  Y8888D'
   7 │        1         2   .d8b.
   8 │        2         2  d8' `8b
   9 │        3         2  88ooo88
  10 │        4         2  88~~~88
  11 │        5         2  88   88
  12 │        6         2  YP   YP
  13 │        1         3  d888888b
  14 │        2         3  `~~88~~'
  15 │        3         3     88
  16 │        4         3     88
  17 │        5         3     88
  18 │        6         3     YP
  19 │        1         4    .d8b.
  20 │        2         4   d8' `8b
  21 │        3         4   88ooo88
  22 │        4         4   88~~~88
  23 │        5         4   88   88
  24 │        6         4   YP   YP

julia> tds = transpose(groupby(ds, :g1), :y)
6×6 Dataset
 Row │ g1        _variables_  _c1        _c2        _c3        _c4       
     │ identity  identity     identity   identity   identity   identity  
     │ Int64?    String?      String?    String?    String?    String?   
─────┼───────────────────────────────────────────────────────────────────
   1 │        1  y            d8888b.     .d8b.     d888888b     .d8b.
   2 │        2  y            88  `8D    d8' `8b    `~~88~~'    d8' `8b
   3 │        3  y            88   88    88ooo88       88       88ooo88
   4 │        4  y            88   88    88~~~88       88       88~~~88
   5 │        5  y            88  .8D    88   88       88       88   88
   6 │        6  y            Y8888D'    YP   YP       YP       YP   YP

julia> mds = map(tds, x->replace(x, r"[^ ]"=>"∑"), r"_c")
6×6 Dataset
 Row │ g1        _variables_  _c1        _c2        _c3        _c4       
     │ identity  identity     identity   identity   identity   identity  
     │ Int64?    String?      String?    String?    String?    String?   
─────┼───────────────────────────────────────────────────────────────────
   1 │        1  y            ∑∑∑∑∑∑∑     ∑∑∑∑∑     ∑∑∑∑∑∑∑∑     ∑∑∑∑∑
   2 │        2  y            ∑∑  ∑∑∑    ∑∑∑ ∑∑∑    ∑∑∑∑∑∑∑∑    ∑∑∑ ∑∑∑
   3 │        3  y            ∑∑   ∑∑    ∑∑∑∑∑∑∑       ∑∑       ∑∑∑∑∑∑∑
   4 │        4  y            ∑∑   ∑∑    ∑∑∑∑∑∑∑       ∑∑       ∑∑∑∑∑∑∑
   5 │        5  y            ∑∑  ∑∑∑    ∑∑   ∑∑       ∑∑       ∑∑   ∑∑
   6 │        6  y            ∑∑∑∑∑∑∑    ∑∑   ∑∑       ∑∑       ∑∑   ∑∑

julia> byrow(mds, sum, r"_c", by = x->count(isequal('∑'),x))
6-element Vector{Union{Missing, Int64}}:
 25
 25
 20
 20
 15
 17

julia> using Chain

julia> @chain mds begin
           repeat!(2)
           sort!(:g1)
           flatten!(r"_c")
           insertcols!(:g2=>repeat(1:9, 12))
           groupby(:g2)
           transpose(r"_c")
           modify!(r"_c"=>byrow(x->join(reverse(x))))
           select!(r"row")
           insertcols!(1, :g=>repeat(1:4, 9))
           sort!(:g)
       end
36×2 Sorted Dataset
 Sorted by: g
 Row │ g         row_function
     │ identity  identity     
     │ Int64?    String?      
─────┼────────────────────────
   1 │        1  ∑∑∑∑∑∑∑∑∑∑∑∑
   2 │        1  ∑∑∑∑∑∑∑∑∑∑∑∑
   3 │        1  ∑∑        ∑∑
   4 │        1  ∑∑        ∑∑
   5 │        1  ∑∑∑∑    ∑∑∑∑
   6 │        1  ∑∑∑∑∑∑∑∑∑∑∑∑
   7 │        1  ∑∑∑∑∑∑∑∑∑∑∑∑
   8 │        1
   9 │        1
  10 │        2  ∑∑∑∑∑∑∑∑∑∑
  11 │        2  ∑∑∑∑∑∑∑∑∑∑∑∑
  12 │        2      ∑∑∑∑∑∑∑∑
  13 │        2      ∑∑∑∑  ∑∑
  14 │        2      ∑∑∑∑∑∑∑∑
  15 │        2  ∑∑∑∑∑∑∑∑∑∑∑∑
  16 │        2  ∑∑∑∑∑∑∑∑∑∑
  17 │        2
  18 │        2
  19 │        3          ∑∑∑∑
  20 │        3          ∑∑∑∑
  21 │        3          ∑∑∑∑
  22 │        3  ∑∑∑∑∑∑∑∑∑∑∑∑
  23 │        3  ∑∑∑∑∑∑∑∑∑∑∑∑
  24 │        3          ∑∑∑∑
  25 │        3          ∑∑∑∑
  26 │        3          ∑∑∑∑
  27 │        3
  28 │        4
  29 │        4  ∑∑∑∑∑∑∑∑∑∑
  30 │        4  ∑∑∑∑∑∑∑∑∑∑∑∑
  31 │        4      ∑∑∑∑∑∑∑∑
  32 │        4      ∑∑∑∑  ∑∑
  33 │        4      ∑∑∑∑∑∑∑∑
  34 │        4  ∑∑∑∑∑∑∑∑∑∑∑∑
  35 │        4  ∑∑∑∑∑∑∑∑∑∑
  36 │        4
```

# Acknowledgement

We like to acknowledge the contributors to `Julia`'s data ecosystem, especially [`DataFrames.jl`](https://github.com/JuliaData/DataFrames.jl), since the existence of their works gave the development of `InMemoryDatasets.jl` a head start.
