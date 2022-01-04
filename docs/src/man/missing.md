# How InMemoryDatasets treats missing values?

## Comparing data sets

`==` of two data sets or two columns fall back to `isequal`.

## Every column supports `missing`

The `Dataset()` constructor automatically converts each column of a data set to allow ‍‍‍‍‍`missing` when constructs a data set. All algorithms in InMemoryDatasets are optimised to minimised the overhead of supporting `missing` type.

## Functions which skip missing values

When InMemoryDatasets loaded into a Julia session, the behaviour of the following functions will be changed in such a way that they will remove missing values if an `AbstractVector{Union{T, Missing}}` is passed as their argument. And it is the user responsibility to handle the situations where this is not desired.

The following list summarises the details of how InMemoryDatasets removes/skips/ignores missing values (for the rest of this section `INTEGERS` refers to `{U/Int8, U/Int16, U/Int32, U/Int64}` and `FLOATS` refers to `{Float16, Float32, Float64}`):

* `argmax` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `argmin` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `cummax` : For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `cummax!`: For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `cummin` : For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `cummin!`: For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `cumprod` : For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `cumprod!`: For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `cumsum` : For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `cumsum!` : For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `extrema` : For `INTEGERS`, `FLOATS`, and `TimeType` skip missing values. When all values are `missing`, it returns `(missing, missing)`.
* `findmax` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `(missing, missing)`.
* `findmin` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `(missing, missing)`.
* `maximum` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `mean` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `median` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `median!`  : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `minimum` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `std` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `sum` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `var` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`

```jldoctest
julia> x = [1,1,missing]
3-element Vector{Union{Missing, Int64}}:
 1
 1
  missing

julia> sum(x)
2

julia> mean(x)
1.0

julia> maximum(x)
1

julia> minimum(x)
1

julia> findmax(x)
(1, 1)

julia> findmin(x)
(1, 1)

julia> cumsum(x)
3-element Vector{Union{Missing, Int64}}:
 1
 2
 2

julia> cumsum(x, missings = :skip)
3-element Vector{Union{Missing, Int64}}:
 1
 2
  missing

julia> cumprod(x, missings = :skip)
3-element Vector{Union{Missing, Int64}}:
 1
 1
  missing

julia> median(x)
1.0
```

### Some remarks

`var` and `std` will return `missing` when `dof = true` and an `AbstractVector{Union{T, Missing}}` of length one is passed as their argument. This is different from the behaviour of these functions defined in the `Statistics` package.

```jldoctest
julia> var(Union{Missing, Int}[1])
missing

julia> std(Union{Missing, Int}[1])
missing

julia> var([1]) # fallback to Statistics.var
NaN

julia> std([1]) # fallback to Statistics.std
NaN
```

## Multithreaded functions

The `sum`, `minimum`, and `maximum` functions also support the `threads` keyword argument. When it is set to `true`, they exploit all cores for calculation.

## `topk`, `IMD.n`, and `IMD.nmissing`

The following function is also exported by InMemoryDatasets:

* `topk` : Return top(bottom) k values of a vector. It ignores `missing` values, unless all values are `missing` which it returns `[missing]`.

and the following functions are not exported but are available via `dot` notation:

* `InMemoryDatasets.n` or `IMD.n` : Return number of non-missing elements
* `InMemoryDatasets.nmissing` or `IMD.nmissing` : Return number of `missing` elements

```jldoctest
julia> x = [13, 1, missing, 10]
4-element Vector{Union{Missing, Int64}}:
 13
  1
   missing
 10

julia> topk(x, 2)
2-element Vector{Int64}:
 13
 10

julia> topk(x, 2, rev = true)
2-element Vector{Int64}:
  1
 10
julia> IMD.n(x)
3

julia> IMD.nmissing(x)
1
```
