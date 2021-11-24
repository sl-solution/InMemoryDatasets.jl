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

julia> mds = map(tds, x->replace(x, r"[^ ]"=>"#"), r"_c")
6×6 Dataset
 Row │ g1        _variables_  _c1        _c2        _c3        _c4       
     │ identity  identity     identity   identity   identity   identity  
     │ Int64?    String?      String?    String?    String?    String?   
─────┼───────────────────────────────────────────────────────────────────
   1 │        1  y            #######     #####     ########     #####
   2 │        2  y            ##  ###    ### ###    ########    ### ###
   3 │        3  y            ##   ##    #######       ##       #######
   4 │        4  y            ##   ##    #######       ##       #######
   5 │        5  y            ##  ###    ##   ##       ##       ##   ##
   6 │        6  y            #######    ##   ##       ##       ##   ##

julia> byrow(mds, sum, r"_c", by = x->count(isequal('#'),x))
6-element Vector{Int64}:
 25
 25
 20
 20
 15
 17

julia> using Chain

julia> @chain tds begin
           vcat(tds)
           sort!(:g1)
           flatten(r"_c")
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
   1 │        1  YY88888888dd
   2 │        1  888888888888
   3 │        1  88        88
   4 │        1  88        88
   5 │        1  88..    ``88
   6 │        1  DD88888888bb
   7 │        1  ''DD8888DD..
   8 │        1
   9 │        1
  10 │        2  YY888888dd
  11 │        2  PP88888888..
  12 │        2      ~~oo''dd
  13 │        2      ~~oo  88
  14 │        2      ~~oo``bb
  15 │        2  YY88888888..
  16 │        2  PP888888bb
  17 │        2
  18 │        2
  19 │        3          ``dd
  20 │        3          ~~88
  21 │        3          ~~88
  22 │        3  YY8888888888
  23 │        3  PP8888888888
  24 │        3          ~~88
  25 │        3          ~~88
  26 │        3          ''bb
  27 │        3
  28 │        4
  29 │        4  YY888888dd
  30 │        4  PP88888888..
  31 │        4      ~~oo''dd
  32 │        4      ~~oo  88
  33 │        4      ~~oo``bb
  34 │        4  YY88888888..
  35 │        4  PP888888bb
  36 │        4
```
