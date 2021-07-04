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
          x == 1 ? "Male" : x == 2 ? "Female" : missing
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

# Modifying a Dataset

The `modify()` function can be used to modify columns or add a transformation of columns to a data set. The syntax of `modify` is 

```julia
modify(ds, op...)
```

where `op` can be of the form `col => fun`, `cols=>fun`, `col=>fun=>:new_name`, `cols=>fun=>:new_names`. Here `fun` is a function which can be applied to one column, i.e. `fun` accepts one column of `ds` and return values by calling `fun` on the selected `col`. When no new names is given the `col` is replaced by the new values. The  feature of `modify` is that from left to right when ever a column is updated or created, the next operation has access to its value (either new or updated values). 

When a row operation is needed to be done, `byrow` can be used instead of `fun`, i.e. `cols => byrow(f, kwargs...)` or `cols => byrow(f, kwargs...)=>:new_name`. In this case `f` is applied to each row of `cols`.

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

julia> modify(ds, 
                 1 => x -> x .^ 2,
                 2:3 => byrow(sqrt) => [:sq_y, :sq_z],
                 [:x, :sq_y] => byrow(-)
              )
10×6 Dataset
 Row │ x         y         z         sq_y      sq_z      row_-
     │ identity  identity  identity  identity  identity  identity
     │ Int64     Int64     Int64     Float64   Float64   Float64
─────┼────────────────────────────────────────────────────────────
   1 │        1         1         1   1.0       1.0       0.0
   2 │        4         1         2   1.0       1.41421   3.0
   3 │        9         2         1   1.41421   1.0       7.58579
   4 │       16         2         2   1.41421   1.41421  14.5858
   5 │       25         3         1   1.73205   1.0      23.2679
   6 │       36         3         2   1.73205   1.41421  34.2679
   7 │       49         4         1   2.0       1.0      47.0
   8 │       64         4         2   2.0       1.41421  62.0
   9 │       81         5         1   2.23607   1.0      78.7639
  10 │      100         5         2   2.23607   1.41421  97.7639


```

In the example above, the value of the first column has been updated and been used in the last operation which itself is based on the calculation from previous operations.

# Grouping Datasets

The function `groupby!(ds, cols; rev = false, issorted = false)` groups a data set (sort `ds` based on `cols`). The sorting and grouping is done based on the formatted values of `cols` rather than the actual values. `combine(ds, args...)` can be used to aggregate the result for each group. The syntax for `args...` is similar to `modify`, with the excption that in `combine` only the variables names can be used to refer to columns in the original or the output data set.

```julia
julia> ds = Dataset(g = [1, 1, 1, 2, 2],
                   x1_int = [0, 0, 1, missing, 2],
                   x2_int = [3, 2, 1, 3, -2],
                   x1_float = [1.2, missing, -1.0, 2.3, 10],
                   x2_float = [missing, missing, 3.0, missing, missing],
                   x3_float = [missing, missing, -1.4, 3.0, -100.0])
5×6 Dataset
 Row │ g         x1_int    x2_int    x1_float   x2_float   x3_float
     │ identity  identity  identity  identity   identity   identity
     │ Int64     Int64?    Int64     Float64?   Float64?   Float64?
─────┼───────────────────────────────────────────────────────────────
   1 │        1         0         3        1.2  missing    missing
   2 │        1         0         2  missing    missing    missing
   3 │        1         1         1       -1.0        3.0       -1.4
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0

julia> groupby!(ds, 1)
5×6 Grouped Dataset with 2 groups
Grouped by: g
 Row │ g         x1_int    x2_int    x1_float   x2_float   x3_float
     │ identity  identity  identity  identity   identity   identity
     │ Int64     Int64?    Int64     Float64?   Float64?   Float64?
─────┼───────────────────────────────────────────────────────────────
   1 │        1         0         3        1.2  missing    missing
   2 │        1         0         2  missing    missing    missing
   3 │        1         1         1       -1.0        3.0       -1.4
─────┼───────────────────────────────────────────────────────────────
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0

julia> combine(ds, :x1_float => sum)
2×2 Grouped Dataset with 2 groups
Grouped by: g
 Row │ g         x1_float
     │ identity  identity
     │ Int64     Float64?
─────┼────────────────────
   1 │        1       0.2
─────┼────────────────────
   2 │        2      12.3
```
