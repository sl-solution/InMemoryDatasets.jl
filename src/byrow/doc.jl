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
    byrow(ds, all, cols; [by = isequal(true), threads, mapformats = false])

Test whether all elements in each row are `true`, when `by` is passed, determine whether predicate `by` returns `true` for all elements in the row.

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
    byrow(ds, any, cols; [by = isequal(true), threads, mapformats = false])

Test whether any elements in each row are `true`, when `by` is passed, determine whether predicate `by` returns `true` for any elements in the row.

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
"""
