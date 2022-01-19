# custom help for byrow()
function Docs.getdoc(x::typeof(byrow), y)
    if y == Union{}
        return _get_doc_byrow("default")
    elseif y == Tuple{typeof(sum)}
        return _get_doc_byrow("sum")
    elseif y == Tuple{typeof(mean)}
        return _get_doc_byrow("mean")
    elseif y == Tuple{typeof(all)}
        return _get_doc_byrow("all")
    elseif y == Tuple{typeof(any)}
        return _get_doc_byrow("any")
    elseif y == Tuple{typeof(count)}
        return _get_doc_byrow("count")
    elseif y == Tuple{typeof(prod)}
        return _get_doc_byrow("prod")
    elseif y == Tuple{typeof(isequal)}
        return _get_doc_byrow("isequal")
    elseif y == Tuple{typeof(isless)}
        return _get_doc_byrow("isless")
    elseif y == Tuple{typeof(in)}
        return _get_doc_byrow("in")
    elseif y == Tuple{typeof(findfirst)}
        return _get_doc_byrow("findfirst")
    elseif y == Tuple{typeof(findlast)}
        return _get_doc_byrow("findlast")
    elseif y == Tuple{typeof(select)}
        return _get_doc_byrow("select")
    end
end



function _get_doc_byrow(fun; text = byrow_docs_text)
    split_text = split(text, "@@@@")
    loc = findfirst(==(fun), split_text)
    Markdown.parse(split_text[loc+1])
end

byrow_docs_text = """
@@@@default@@@@
    byrow(ds, fun, cols; ...)

Perform a row-wise operation specified by `fun` on selected columns `cols`. Generally,
`fun` can be any function that returns a scalar value for each row.

`byrow` is fine tuned for the following operations, to get extra help for each of them search help for `byrow(fun)`, e.g. `?byrow(sum)`;

# Reduction operations

- `all`
- `any`
- `argmax`
- `argmin`
- `coalesce`
- `count`
- `findfirst`
- `findlast`
- `hash`
- `in`
- `isequal`
- `isless`
- `issorted`
- `join`
- `mapreduce`
- `maximum`
- `mean`
- `minimum`
- `nunique`
- `prod`
- `select`
- `std`
- `sum`
- `var`

# Special operations

- `cummax`
- `cummax!`
- `cummin`
- `cummin!`
- `cumprod`
- `cumprod!`
- `cumsum`
- `cumsum!`
- `fill`
- `fill!`
- `sort`
- `sort!`
- `stdze`
- `stdze!`
@@@@sum@@@@
    byrow(ds, sum, cols = names(ds, Number); [by = identity, threads])

Sum results of calling function `by` on each element of each row of `ds`. If `cols` is not specified, `byrow`
computes sum for all numeric columns in `ds`.

Passing `threads = false` disables multitrheaded computations.

Missing values are removed from the calculation. When all values in a row are missing, it returns `missing`.

## Example

```jldoctest
julia> ds = Dataset(x = [1,2,3], y = [2.0, 1.5, 4.0])
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1       2.0
   2 │        2       1.5
   3 │        3       4.0

julia> byrow(ds, sum, :)
3-element Vector{Float64}:
 3.0
 3.5
 7.0
```
@@@@mean@@@@
    byrow(ds, mean, cols = names(ds, Number); [by = identity, threads])

Compute mean of the results of calling function `by` on each element of each row of `ds`. If `cols` is not specified, `byrow`
computes mean for all numeric columns in `ds`.

Passing `threads = false` disables multitrheaded computations.

Missing values are removed from the calculation. When all values in a row are missing, it returns `missing`.

## Example

```jldoctest
julia> ds = Dataset(x = [1,2,3], y = [2.0, 1.5, 4.0])
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1       2.0
   2 │        2       1.5
   3 │        3       4.0

julia> byrow(ds, mean, :)
3-element Vector{Float64}:
 1.5
 1.75
 3.5
```
@@@@all@@@@
    byrow(ds, all, cols = :; [by = isequal(true), threads, mapformats = false])

Test whether all elements in each row in selected columns are `true`, when `by` is passed, determine whether predicate `by` returns `true` for all elements in the row.

By default, `byrow` uses the actual values for test the elements, however, passing `mapformats = true`
change this to the formatted values.

Each columns in `cols` may have its own `by`. This may be achieved by passing a vector of predicates to `by`.

Passing `threads = false` disables multitrheaded computations.

See [`filter`](@ref) or [`filter!`](@ref)

## Example

```jldoctest
julia> ds = Dataset(x = [1,2,3], y = [2.0, 1.5, 4.0])
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1       2.0
   2 │        2       1.5
   3 │        3       4.0

julia> byrow(ds, all, :, by = [==(2), >(1)])
3-element Vector{Bool}:
 0
 1
 0
```
@@@@any@@@@
    byrow(ds, any, cols = :; [by = isequal(true), threads, mapformats = false])

Test whether any elements in each row in selected columns is `true`, when `by` is passed, determine whether predicate `by` returns `true` for any elements in the row.

By default, `byrow` uses the actual values for test the elements, however, passing `mapformats = true`
change this to the formatted values.

Each columns in `cols` may have its own `by`. This may be achieved by passing a vector of predicates to `by`.

Passing `threads = false` disables multitrheaded computations.

See [`filter`](@ref) or [`filter!`](@ref)

## Example

```jldoctest
julia> ds = Dataset(x = [1,2,3], y = [2.0, 1.5, 4.0])
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1       2.0
   2 │        2       1.5
   3 │        3       4.0

julia> byrow(ds, any, :, by = [==(2), >(1)])
3-element Vector{Bool}:
 1
 1
 1
```
@@@@count@@@@
    byrow(ds, count, cols = :; [by = isequal(true), threads])

Count the number of elements in each row for selected columns which the function `by` returns `true`.

Passing `threads = false` disables multitrheaded computations.

## Example

```jldoctest
julia> julia> ds = Dataset(x = [1,2,3], y = [2, 6, 5])
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         2
   2 │        2         6
   3 │        3         5

julia> byrow(ds, count, :, by = isodd)
3-element Vector{Int32}:
 1
 0
 2
```
@@@@prod@@@@
    byrow(ds, prod, cols = names(ds, Number); [by = identity, threads])

Return the product of the results of calling function `by` on each element of each row of `ds`. If `cols` is not specified, `byrow`
computes product for all numeric columns in `ds`.

Passing `threads = false` disables multitrheaded computations.

Missing values are removed from the calculation. When all values in a row are missing, it returns `missing`.

## Example

```jldoctest
julia>  ds = Dataset(x = [1,2,3], y = [2.0, 1.5, 4.0])
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1       2.0
   2 │        2       1.5
   3 │        3       4.0

julia> byrow(ds, prod, :)
3-element Vector{Union{Missing, Float64}}:
  2.0
  3.0
 12.0
```
@@@@isequal@@@@
    byrow(ds, isequal, cols; [with = nothing, threads])

Returns a boolean vector which is `true` if all values in the corresponding row are equal (using `isequal`).
 Optionally, a vector of values can be passed view the `with` keyword argument to compare values in selected
 columns with the passed vector.

Passing `threads = false` disables multitrheaded computations.

See [`byrow(isless)`](@ref), [`byrow(in)`](@ref), [`byrow(issorted)`](@ref)

## Examples

```jldoctest
julia> ds = Dataset(x1 = [1,2,3,1,2,3], x2 = [1,2,1,2,1,2])
6×2 Dataset
 Row │ x1        x2
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        3         1
   4 │        1         2
   5 │        2         1
   6 │        3         2

julia> byrow(ds, isequal, [1,2])
6-element Vector{Bool}:
 1
 1
 0
 0
 0
 0

julia> byrow(ds, isequal, [1,2], with = [2,2,2,3,3,3])
6-element Vector{Bool}:
 0
 1
 0
 0
 0
 0
```
@@@@isless@@@@
    byrow(ds, isless, cols, [with, threads, rev = false, lt = isless])

Return a boolean vector which is true if all values in corresponding row for selected `cols` are less than value given by the `with` keyword argument. A vector, or a column name can be passed via `with`.

Passing `rev = true` returns true if all values are greater than passed values via `with`.

By default, the comparison is done via `isless` function, however, user may change it by passing a function via the `lt` keyword argument. The function passed to `lt` must accept two arguments where it takes its first argument from `cols` and its second argument from `with`. However, if `rev = true` the function passed as `lt` will take its first argument from `with` and its second argument from `cols`. The function passed as `lt` must return `true` or `false`.

Passing `threads = false` disables multitrheaded computations.

See [`byrow(isequal)`](@ref), [`byrow(in)`](@ref), [`byrow(issorted)`](@ref)

## Examples

```jldoctest
julia> ds = Dataset(x1 = [1,2,3,1,2,3], x2 = [1,2,1,2,1,2], x3 = 6:-1:1)
6×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         1         6
   2 │        2         2         5
   3 │        3         1         4
   4 │        1         2         3
   5 │        2         1         2
   6 │        3         2         1

julia> byrow(ds, isless, [1,2], with = :x3)
6-element Vector{Bool}:
 1
 1
 1
 1
 0
 0

julia> byrow(ds, isless, 1:2, with = :x3, lt = (x,y) -> isless(x^2, y))
6-element Vector{Bool}:
 1
 1
 0
 0
 0
 0

julia> ds = Dataset(x1 = [1,2,3,1,2,3], x2 = [1,2,1,2,1,2],
                    x3 = [(1,2), (2,3), (1,2), (4,5), (1,2), (1,2)])
6×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Tuple…?
─────┼──────────────────────────────
   1 │        1         1  (1, 2)
   2 │        2         2  (2, 3)
   3 │        3         1  (1, 2)
   4 │        1         2  (4, 5)
   5 │        2         1  (1, 2)
   6 │        3         2  (1, 2)

julia> byrow(ds, isless, 1:2, with = :x3, lt = in)
6-element Vector{Bool}:
 1
 1
 0
 0
 1
 0
```
@@@@in@@@@
    byrow(ds, in, cols; [item, threads, eq = isequal])

Return a boolean vector which its elements are true if in a row the value of `item` is equal to any values from `cols`. The equality is checked via the function passed as `eq`. User can pass a vector of values or a column name to `item`.

The function passed as `eq` must accept two arguments where it takes its first argument from `item` and its second argument from `cols`. The function passed as `eq` must return `true` or `false`.

Passing `threads = false` disables multitrheaded computations.

See [`byrow(isequal)`](@ref), [`byrow(isless)`](@ref), [`byrow(issorted)`](@ref)

## Examples

```jldoctest
julia> ds = Dataset(x1 = [1,2,3,1,2,3], x2 = [1,2,1,2,1,2], x3 = 6:-1:1)
6×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         1         6
   2 │        2         2         5
   3 │        3         1         4
   4 │        1         2         3
   5 │        2         1         2
   6 │        3         2         1

julia> byrow(ds, in, r"x", item = [1,2,3,4,5,6])
6-element Vector{Bool}:
 1
 1
 1
 0
 0
 0

julia> byrow(ds, in, [2,3], item = :x1, eq = isless)
6-element Vector{Bool}:
 1
 1
 1
 1
 0
 0
julia> byrow(ds, in, r"x", item = [5,4,5,4,5,4], eq = (x,y) -> x+y == 11)
6-element Vector{Bool}:
 1
 0
 0
 0
 0
 0
```
@@@@findfirst@@@@
    byrow(ds, findfirst, cols; [by = identity, item = nothing, eq = isequal, threads])

Return the column name of the first `true` value in `cols` or for which `by` returns `true`. If no such value is found, it returns `missing`. User can pass a vector of values or a column name to `item` to find the column name of the first time that the value of `item` is equal to the value of the column. User may use a customised function for checking the equlity of `item` and `columns` by passing it to the `eq` keyword argument. The function passed as `eq` must be a binary function where its first argument is from `item` and its second argument is from `col`.

Passing `threads = false` disables multitrheaded computations.

See [`byrow(findlast)`](@ref), [`byrow(select)`](@ref)

# Examples
```jldoctest
julia> ds = Dataset(g = [1, 1, 1, 2, 2],
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
   1 │        1         0         3        1.2  missing    missing
   2 │        1         0         2  missing    missing    missing
   3 │        1         1         1       -1.0        3.0       -1.4
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0

julia> byrow(ds, findfirst, :, by = ismissing)
5-element PooledArrays.PooledVector{Union{Missing, Symbol}, UInt32, Vector{UInt32}}:
 :x2_float
 :x1_float
 missing
 :x1_int
 :x2_float

julia> byrow(ds, findfirst, 1:3, item = [1,1,1,1,1])
5-element PooledArrays.PooledVector{Union{Missing, Symbol}, UInt32, Vector{UInt32}}:
 :g
 :g
 :g
 missing
 missing

julia> ds = Dataset(x1 = [1,2,2], x2 = [5,6,7], x3 = [8,9,10])
3×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         5         8
   2 │        2         6         9
   3 │        2         7        10

julia> byrow(ds, select, :, with = byrow(ds, findfirst, :, by = isodd))
3-element Vector{Union{Missing, Int64}}:
 1
 9
 7
```
@@@@findlast@@@@
    byrow(ds, findlast, cols; [by = identity, item = nothing, eq = isequal, threads])

Return the column name of the last `true` value in `cols` or for which `by` returns `true`. If no such value is found, it returns `missing`. User can pass a vector of values or a column name to `item` to find the column name of the last time that the value of `item` is equal to the value of the column. User may use a customised function for checking the equlity of `item` and `columns` by passing it to the `eq` keyword argument. The function passed as `eq` must be a binary function where its first argument is from `item` and its second argument is from `col`.

Passing `threads = false` disables multitrheaded computations.

See [`byrow(findfirst)`](@ref), [`byrow(select)`](@ref)

# Examples
```jldoctest
julia> ds = Dataset(g = [1, 1, 1, 2, 2],
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
   1 │        1         0         3        1.2  missing    missing
   2 │        1         0         2  missing    missing    missing
   3 │        1         1         1       -1.0        3.0       -1.4
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0

julia> byrow(ds, findlast, :, by = ismissing)
5-element PooledArrays.PooledVector{Union{Missing, Symbol}, UInt32, Vector{UInt32}}:
 :x3_float
 :x3_float
 missing
 :x2_float
 :x2_float

julia> byrow(ds, findlast, 1:3, item = [1,1,1,1,1])
5-element PooledArrays.PooledVector{Union{Missing, Symbol}, UInt32, Vector{UInt32}}:
 :g
 :g
 :x2_int
 missing
 missing

julia> ds = Dataset(x1 = [1,2,2], x2 = [5,6,7], x3 = [8,9,10])
3×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         5         8
   2 │        2         6         9
   3 │        2         7        10

julia> byrow(ds, select, :, with = byrow(ds, findlast, :, by = isodd))
3-element Vector{Union{Missing, Int64}}:
 5
 9
 7
```
@@@@select@@@@
    byrow(ds, select, cols; [with, threads])

Select value of `with` among `cols`. The `with` must be a vector of column names(`Symbol` or `String`) or column index (relative to column position in `cols`) or a column name which contains this information.

For heterogeneous column types, `byrow` use `promote_type` for the output. If the column select doesn't exist among `cols`, `byrow` returns `missing`.

Passing `threads = false` disables multitrheaded computations.

See [`byrow(findfirst)`](@ref), [`byrow(findlast)`](@ref)

# Examples
```jldoctest
julia> ds = Dataset(x1 = [1,2,3,4],
            x2 = [1.5,6.5,3.4,2.4],
            x3 = [true, false, true, false],
            y1 = ["x2", "x1", missing, "x2"],
            y2 = [:x2, :x1, missing, :x2],
            y3 = [3,1,1,2])
4×6 Dataset
 Row │ x1        x2        x3        y1        y2        y3
     │ identity  identity  identity  identity  identity  identity
     │ Int64?    Float64?  Bool?     String?   Symbol?   Int64?
─────┼────────────────────────────────────────────────────────────
   1 │        1       1.5      true  x2        x2               3
   2 │        2       6.5     false  x1        x1               1
   3 │        3       3.4      true  missing   missing          1
   4 │        4       2.4     false  x2        x2               2

julia> byrow(ds, select, 1:2, with = :y1)
4-element Vector{Union{Missing, Float64}}:
 1.5
 2.0
  missing
 2.4

julia> byrow(ds, select, [2,1,3], with = :y3)
4-element Vector{Union{Missing, Float64}}:
 1.0
 6.5
 3.4
 4.0

julia> byrow(ds, select, [2,1,3], with = [3,1,1,2])
4-element Vector{Union{Missing, Float64}}:
 1.0
 6.5
 3.4
 4.0

julia> ds = Dataset(x1 = [1,2,2], x2 = [5,6,7], x3 = [8,9,10])
3×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         5         8
   2 │        2         6         9
   3 │        2         7        10

julia> byrow(ds, select, :, with = byrow(ds, findfirst, :, by = isodd))
3-element Vector{Union{Missing, Int64}}:
 1
 9
 7
```
"""
