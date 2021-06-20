# Datasets

`InMemoryDatasets.jl` is a `Julia` package for working with tabular data sets.

# Examples

```julia
> using InMemoryDatasets
> a = Dataset(x = [1,2], y = [1,2])
2×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1         1
   2 │        2         2
```

# Formats

For each data set, one can assign a named function to a column as its format. The column formatted values will be used for displaying, sorting, grouping and joining, however, for any other operation the actual values will be used. The format function doesn't modify the actual values of a column.

`setformat!` assigns a format to a column, and `removeformat!` removes a column format.

## Examples

```julia
julia> ds = Dataset(randn(10,2), :auto)
10×2 Dataset
 Row │ x1          x2        
     │ identity    identity  
     │ Float64     Float64   
─────┼───────────────────────
   1 │  0.108189   -2.71151
   2 │ -0.520872   -1.00426
   3 │  0.667433   -0.357071
   4 │ -0.317271   -0.457264
   5 │  0.404249    0.405335
   6 │ -1.0304      0.292216
   7 │  0.874799   -0.169534
   8 │  0.0723834   1.47378
   9 │  0.338568    1.08032
  10 │ -1.07939     1.24903

julia> myformat(x) = round(Int, x)
myformat (generic function with 1 method)

julia>  setformat!(ds, 1 => myformat)
10×2 Dataset
 Row │ x1        x2        
     │ myformat  identity  
     │ Float64   Float64   
─────┼─────────────────────
   1 │        0  -2.71151
   2 │       -1  -1.00426
   3 │        1  -0.357071
   4 │        0  -0.457264
   5 │        0   0.405335
   6 │       -1   0.292216
   7 │        1  -0.169534
   8 │        0   1.47378
   9 │        0   1.08032
  10 │       -1   1.24903

julia> getformat(ds, :x1)
myformat (generic function with 1 method)

julia> removeformat!(ds, :x1)
10×2 Dataset
 Row │ x1          x2        
     │ identity    identity  
     │ Float64     Float64   
─────┼───────────────────────
   1 │  0.108189   -2.71151
   2 │ -0.520872   -1.00426
   3 │  0.667433   -0.357071
   4 │ -0.317271   -0.457264
   5 │  0.404249    0.405335
   6 │ -1.0304      0.292216
   7 │  0.874799   -0.169534
   8 │  0.0723834   1.47378
   9 │  0.338568    1.08032
  10 │ -1.07939     1.24903
```

# Masking observations

The `mask(ds, fun, cols)` function can be used to return a Bool `Dataset` which the observation in row `i` and column `j` is true if `fun(ds[i, j]` is true, otherwise it is false. The `fun` is called on actual values by default, however, using the option `mapformats = true` causes `fun` to be called on the formatted values.

## Examples

```julia
julia> ds = Dataset(x = 1:10, y = repeat(1:5, inner = 2), z = repeat(1:2, 5))
10×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64     Int64     Int64
─────┼──────────────────────────────
   1 │        1         1         1
   2 │        2         1         2
   3 │        3         2         1
   4 │        4         2         2
   5 │        5         3         1
   6 │        6         3         2
   7 │        7         4         1
   8 │        8         4         2
   9 │        9         5         1
  10 │       10         5         2

julia> function gender(x)
            dict = Dict(1 => "Male", 2 => "Female")
            get(dict, x, x)
        end
julia> setformat!(ds, 2 => sqrt, 3 => gender)
10×3 Dataset
 Row │ x         y        z
     │ identity  sqrt     gender
     │ Int64     Int64    Int64
─────┼───────────────────────────
   1 │        1  1.0        Male
   2 │        2  1.0      Female
   3 │        3  1.41421    Male
   4 │        4  1.41421  Female
   5 │        5  1.73205    Male
   6 │        6  1.73205  Female
   7 │        7  2.0        Male
   8 │        8  2.0      Female
   9 │        9  2.23607    Male
  10 │       10  2.23607  Female

julia> mask(ds, [iseven, isequal("Male")], 2:3)
10×2 Dataset
 Row │ y         z
     │ identity  identity
     │ Bool      Bool
─────┼────────────────────
   1 │    false     false
   2 │    false     false
   3 │     true     false
   4 │     true     false
   5 │    false     false
   6 │    false     false
   7 │     true     false
   8 │     true     false
   9 │    false     false
  10 │    false     false

julia> mask(ds, [val -> rem(val, 2) == 0, isequal("Male")], 2:3, mapformats = true)
10×2 Dataset
 Row │ y         z
     │ identity  identity
     │ Bool      Bool
─────┼────────────────────
   1 │    false      true
   2 │    false     false
   3 │    false      true
   4 │    false     false
   5 │    false      true
   6 │    false     false
   7 │     true      true
   8 │     true     false
   9 │    false      true
  10 │    false     false
```
