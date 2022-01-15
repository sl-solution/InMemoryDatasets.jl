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

See [`byrow(isless)`](@ref) and [`byrow(in)`](@ref)

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

See [`byrow(isequal)`](@ref) and [`byrow(in)`](@ref)

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

See [`byrow(isequal)`](@ref) and [`byrow(isless)`](@ref)

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
"""
