# Function `map/map!`

## Introduction

There are multiple ways to call a function on individual values (observations) of a column. In this section we discuss two of them: `map!` and `map`. These two functions are high-performance and customised version of `map!` and `map` in Julia Base.

> `map!` and `map` call functions on the actual values, i.e. if the column is formatted the functions are called on the values which may be different from what you see when show a data set.

## `map!`

The InMemoryDatasets' `map!` function modifies a data set in-place by calling a function on individual values of a column. The syntax for the function is

> `map!(ds, fun, cols; threads = true)`

where `ds` is a data set, and `cols` can be a single column or multiple columns selector. When `fun` is a single function, `map!` call that function on all values of `cols`. Since `map!` is modifying the data set `ds` in-place, it will skip columns in `cols` where `fun` changes the type of columns, and print a warning on the screen.

The `threads = true` keyword argument causes `map!` to exploit all cores available to Julia for doing the calculations. This is particularly useful when the `fun` function is expensive to calculate.

When `fun` is a vector of functions and `cols` refers to multiple columns, `map!` calls each element of `fun` to the values of the corresponding column in `cols`. this means that it is possible to call different functions on different columns of a data set, however, the length of `fun` and the number of selected columns must be the same.

### Examples

In the following data set we like to replace every `missing` in `:x2` and `:x3` with value `0`. Thus, we use `map!` and a suitable function to do this:

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [-2, -1, missing, 1, 2],
                    x3 = [0.0, 0.1, 0.2, missing, 0.4])
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1        -2       0.0
   2 │        2        -1       0.1
   3 │        3   missing       0.2
   4 │        4         1   missing
   5 │        5         2       0.4

julia> map!(ds, x -> ismissing(x) ? 0 : x, 2:3)
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1        -2       0.0
   2 │        2        -1       0.1
   3 │        3         0       0.2
   4 │        4         1       0.0
   5 │        5         2       0.4
```

Now let's call `sqrt` on all values. Note that `sqrt` of an integer is a float and `sqrt` of negative integer is not valid in Julia. So `map!` only applies the provided function on the last column and skips the first two columns.

```jldoctest
julia> map!(ds, sqrt, :)
┌ Warning: cannot map `f` on ds[!, :x1] in-place, the selected column is Union{Missing, Int64} and the result of calculation is Union{Missing, Float64}
└ @ InMemoryDatasets ~/.julia/dev/InMemoryDatasets/src/dataset/other.jl:394
┌ Warning: cannot map `f` on ds[!, :x2] in-place, the selected column is Union{Missing, Int64} and the result of calculation is Union{Missing, Float64}
└ @ InMemoryDatasets ~/.julia/dev/InMemoryDatasets/src/dataset/other.jl:394
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1        -2  0.0
   2 │        2        -1  0.316228
   3 │        3         0  0.447214
   4 │        4         1  0.0
   5 │        5         2  0.632456
```

As another example let's look at a data set where a column already has a `format`.

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [1,2,1,2,1])
julia> setformat!(ds, 1=>isodd)
5×2 Dataset
 Row │ x1      x2
     │ isodd   identity
     │ Int64?  Int64?
─────┼──────────────────
   1 │   true         1
   2 │  false         2
   3 │   true         1
   4 │  false         2
   5 │   true         1

julia> map!(ds, x->div(x,2), 1:2)
5×2 Dataset
 Row │ x1      x2
     │ isodd   identity
     │ Int64?  Int64?
─────┼──────────────────
   1 │  false         0
   2 │   true         1
   3 │   true         0
   4 │  false         1
   5 │  false         0
```

Note that the `format` of `:x1` is preserved and the function call changed the actual values. Thus, Datasets applies the `format` of `:x1` to new values.

The following example shows how different functions can be used for different columns.

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [-2, -1, missing, 1, 2],
                    x3 = [0.0, 0.1, 0.2, missing, 0.4])
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1        -2       0.0
   2 │        2        -1       0.1
   3 │        3   missing       0.2
   4 │        4         1   missing
   5 │        5         2       0.4

julia> fun_vec = [x -> div(x, 2),
                  x -> ismissing(x) ? 0 : x,
                  sqrt]
3-element Vector{Function}:
 #5 (generic function with 1 method)
 #6 (generic function with 1 method)
 sqrt (generic function with 19 methods)

julia> map!(ds, fun_vec, 1:3)
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼────────────────────────────────────
   1 │        0        -2        0.0
   2 │        1        -1        0.316228
   3 │        1         0        0.447214
   4 │        2         1  missing
   5 │        2         2        0.632456
```

## `map`

The `map` function also calls a function (or a vector of functions) on single or muliple columns of a data set. However, it differs from `map!` that we discussed earlier in three main areas,

- it doesn't do in-place operation and returns a copy of the data set,
- unlike `map!`, it can change the type of columns, and
- it doesn't preserve the `format`s of the columns that are going to be modified.

### Examples

In the following example we call `x -> x/2` on all columns. However, note that `map` automatically changes the data type of the first two columns, and more importantly it **doesn't** modify the original data set:

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [-2, -1, missing, 1, 2],
                    x3 = [0.0, 0.1, 0.2, missing, 0.4])
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1        -2       0.0
   2 │        2        -1       0.1
   3 │        3   missing       0.2
   4 │        4         1   missing
   5 │        5         2       0.4
julia> map(ds, x -> x/2, r"x")
5×3 Dataset
 Row │ x1        x2         x3
     │ identity  identity   identity
     │ Float64?  Float64?   Float64?
─────┼─────────────────────────────────
   1 │      0.5       -1.0        0.0
   2 │      1.0       -0.5        0.05
   3 │      1.5  missing          0.1
   4 │      2.0        0.5  missing
   5 │      2.5        1.0        0.2

julia> ds # map doesn't modify the original data set
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼───────────────────────────────
   1 │        1        -2        0.0
   2 │        2        -1        0.1
   3 │        3   missing        0.2
   4 │        4         1  missing
   5 │        5         2        0.4
```

In the following example, we `map` some functions on columns of a data set which one of the columns has a `format`.

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [1,2,1,2,1])
julia> setformat!(ds, 1=>isodd)
5×2 Dataset
 Row │ x1      x2
     │ isodd   identity
     │ Int64?  Int64?
─────┼──────────────────
   1 │   true         1
   2 │  false         2
   3 │   true         1
   4 │  false         2
   5 │   true         1

julia> map(ds, [x -> div(x,2), x -> x/2], :)
5×2 Dataset
 Row │ x1        x2
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        0       0.5
   2 │        1       1.0
   3 │        1       0.5
   4 │        2       1.0
   5 │        2       0.5
```
