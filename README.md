# Datasets

`InMemoryDatasets.jl` is a `Julia` package for working with tabular data sets.

[![](https://img.shields.io/badge/docs-stable-blue.svg)](https://sl-solution.github.io/InMemoryDatasets.jl/stable)

# Example

```jldoctest
julia> using InMemoryDatasets
julia> g1 = repeat(1:6, inner = 4);
julia> g2 = repeat(1:4, 6);
julia> y = ["d8888b.  ", " .d8b.   ", "d888888b ", "  .d8b.  ", "88  `8D  ", "d8' `8b  ", "`~~88~~' ", " d8' `8b ", "88   88  ", "88ooo88  ", "   88    ", " 88ooo88 ", "88   88  ", "88~~~88  ", "   88    ", " 88~~~88 ", "88  .8D  ", "88   88  ", "   88    ", " 88   88 ", "Y8888D'  ", "YP   YP  ", "   YP    ", " YP   YP "];
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
  
julia> sort(ds, 2)
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

julia> tds = transpose(groupby(ds, 1), :y)
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
julia> mds = map(tds, x->replace(x, r"[^ ]"=>"*"), r"_c")
6×6 Dataset
 Row │ g1        _variables_  _c1        _c2        _c3        _c4       
     │ identity  identity     identity   identity   identity   identity  
     │ Int64?    String?      String?    String?    String?    String?   
─────┼───────────────────────────────────────────────────────────────────
   1 │        1  y            *******     *****     ********     *****
   2 │        2  y            **  ***    *** ***    ********    *** ***
   3 │        3  y            **   **    *******       **       *******
   4 │        4  y            **   **    *******       **       *******
   5 │        5  y            **  ***    **   **       **       **   **
   6 │        6  y            *******    **   **       **       **   **
julia> byrow(mds, sum, r"_c", by = x->count(isequal('*'),x))
6-element Vector{Int64}:
 25
 25
 20
 20
 15
 17
```

