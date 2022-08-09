# How InMemoryDatasets treats missing values?

## Comparing data sets

`==` of two data sets or two `DatasetColumn`s falls back to `isequal`.

## Every column supports `missing`

The `Dataset()` constructor automatically converts each column of a data set to allow ‍‍‍‍‍`missing` when constructs a data set. All algorithms in InMemoryDatasets are optimised to minimised the overhead of supporting `missing` type.

## Functions which skip missing values

InMemoryDatasets has a set of functions which removes missing values. The following list summarises the details of how InMemoryDatasets removes/skips/ignores missing values (for the rest of this section `INTEGERS` refers to `{U/Int8, U/Int16, U/Int32, U/Int64}` and `FLOATS` refers to `{Float16, Float32, Float64}`):

* `IMD.argmax` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `IMD.argmin` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `IMD.cummax` : For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.cummax!`: For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.cummin` : For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.cummin!`: For `INTEGERS`, `FLOATS`, and `TimeType` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.cumprod` : For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.cumprod!`: For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.cumsum` : For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.cumsum!` : For `INTEGERS` and `FLOATS` ignore missing values, however, by passing `missings = :skip` it jumps over missing values. When all values are `missing`, it returns the input.
* `IMD.extrema` : For `INTEGERS`, `FLOATS`, and `TimeType` skip missing values. When all values are `missing`, it returns `(missing, missing)`.
* `IMD.findmax` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `(missing, missing)`.
* `IMD.findmin` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `(missing, missing)`.
* `IMD.maximum` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `IMD.mean` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `IMD.median` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `IMD.median!`  : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `IMD.minimum` : For `INTEGERS`, `FLOATS`, `TimeType`, and `AbstractString` skip missing values. When all values are `missing`, it returns `missing`.
* `IMD.std` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `IMD.sum` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`
* `IMD.var` : For `INTEGERS` and `FLOATS` skip missing values. When all values are `missing`, it returns `missing`

```jldoctest
julia> x = [1,1,missing]
3-element Vector{Union{Missing, Int64}}:
 1
 1
  missing

julia> IMD.sum(x)
2

julia> IMD.mean(x)
1.0

julia> IMD.maximum(x)
1

julia> IMD.minimum(x)
1

julia> IMD.findmax(x)
(1, 1)

julia> IMD.findmin(x)
(1, 1)

julia> IMD.cumsum(x)
3-element Vector{Union{Missing, Int64}}:
 1
 2
 2

julia> IMD.cumsum(x, missings = :skip)
3-element Vector{Union{Missing, Int64}}:
 1
 2
  missing

julia> IMD.cumprod(x, missings = :skip)
3-element Vector{Union{Missing, Int64}}:
 1
 1
  missing

julia> IMD.median(x)
1.0
```

### Some remarks

`var` and `std` will return `missing` when `dof = true` and an `AbstractVector` of length one is passed as their argument. This is different from the behaviour of these functions defined in the `Statistics` package.

```jldoctest
julia> IMD.var([1])
missing

julia> IMD.std([1])
missing

julia> Statistics.var([1])
NaN

julia> Statistics.std([1])
NaN
```

## Multithreaded functions

The `IMD.sum`, `IMD.minimum`, and `IMD.maximum` functions also support the `threads` keyword argument. When it is set to `true`, they exploit all cores for calculation.

## Other functions

The following functions are also exported by InMemoryDatasets:

* `bfill` : backward filling
* `bfill!` : backward filling in-place
* `ffill` : forward filling
* `ffill!` : forward filling in-place
* `lag` : Create a lag-k of the provided vector
* `lag!` : Replace its input with a lag-k values
* `lead` : Create a lead-k of the provided vector
* `lead!` : Replace its input with a lead-k values
* `topk` : Return top(bottom) k values of a vector. It ignores `missing` values, unless all values are `missing` which it returns `[missing]`.
* `topkperm` :  Return the indices of top(bottom) k values of a vector. It ignores `missing` values, unless all values are `missing` which it returns `[missing]`.

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
