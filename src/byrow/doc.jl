function Docs.getdoc(x::typeof(byrow), y)
    if y == Union{}
        return Markdown.parse("""
byrow(ds, fun, cols, ...)

Perform a row-wise operation specified by `fun` on selected columns `cols`. Generally,
`fun` can be any function that returns a scalar value for each row.

`byrow` is fine tuned for the following operations, to get extra help for each of them search help for `byrow(fun)`;

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

""")
    elseif y == Tuple{typeof(sum)}
        return Markdown.parse("""
byrow(ds, sum, cols = names(ds, Number); [by = identity, threads])

Sum the results of calling function `by` on each element of each row of `ds`. If `cols` is not specified, `byrow`
computes sum for all numeric columns in `ds`.

Passing `threads = false` disable multitrheaded computations.

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
3-element Vector{Union{Missing, Float64}}:
 3.0
 3.5
 7.0
```
""")
    elseif y == Tuple{typeof(mean)}
        return Markdown.parse("""
byrow(ds, sum, cols = names(ds, Number); [by = identity, threads])

Compute mean of the results of calling function `by` on each element of each row of `ds`. If `cols` is not specified, `byrow`
computes mean for all numeric columns in `ds`.

Passing `threads = false` disable multitrheaded computations.

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
""")
elseif y == Tuple{typeof(all)}
    return Markdown.parse("""
    byrow(ds, all, cols; [by = isequal(true), threads, mapformats = false])

Test whether all elements in each row are `true`, when `by` is passed, determine whether predicate `by` returns `true` for all elements in the row.

By default, `byrow` uses the actual values for test the elements, however, passing `mapformats = true`
change this to the formatted values.

Each columns in `cols` may have its own `by`. This may be achieved by passing a vector of predicates to `by`.

Passing `threads = false` disable multitrheaded computations.

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
""")

    end
end
