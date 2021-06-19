##############################################################################
##
## Missing values support
##
##############################################################################

"""
    allowmissing!(ds::Dataset, cols=:)

Convert columns `cols` of data set `ds` from element type `T` to
`Union{T, Missing}` to support missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data set are converted.
"""
function allowmissing! end

# Modify Dataset
function allowmissing!(ds::Dataset, col::ColumnIndex)
    colidx = index(ds)[col]
    # use _columns to avoid reseting format or grouping info
    _columns(ds)[colidx] = allowmissing(_columns(ds)[colidx])
    _modified(_attributes(ds))
    return ds
end

# Modify Dataset
function allowmissing!(ds::Dataset, cols::AbstractVector{<:ColumnIndex})
    for col in cols
        allowmissing!(ds, col)
    end
    return ds
end

# Modify Dataset
function allowmissing!(ds::Dataset, cols::AbstractVector{Bool})
    length(cols) == size(ds, 2) || throw(BoundsError(ds, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && allowmissing!(ds, col)
    end
    return ds
end

# Modify Dataset
allowmissing!(ds::Dataset, cols::MultiColumnIndex) =
    allowmissing!(ds, index(ds)[cols])

# Modify Dataset
allowmissing!(ds::Dataset, cols::Colon=:) =
    allowmissing!(ds, axes(ds, 2))

"""
    disallowmissing!(ds::Dataset, cols=:; error::Bool=true)

Convert columns `cols` of data set `ds` from element type `Union{T, Missing}` to
`T` to drop support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data set are converted.

If `error=false` then columns containing a `missing` value will be skipped instead
of throwing an error.
"""
function disallowmissing! end

# Modify Dataset
function disallowmissing!(ds::Dataset, col::ColumnIndex; error::Bool=true)
    x = ds[!, col]
    colidx = index(ds)[col]
    if !(!error && Missing <: eltype(x) && any(ismissing, x))
        # use _columns to avoid reseting attributes
         _columns(ds)[colidx] = disallowmissing(x)
    end
    _modified(_attributes(ds))
    return ds
end

# Modify Dataset
function disallowmissing!(ds::Dataset, cols::AbstractVector{<:ColumnIndex};
                          error::Bool=true)
    for col in cols
        disallowmissing!(ds, col, error=error)
    end
    return ds
end

# Modify Dataset
function disallowmissing!(ds::Dataset, cols::AbstractVector{Bool}; error::Bool=true)
    length(cols) == size(ds, 2) || throw(BoundsError(ds, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && disallowmissing!(ds, col, error=error)
    end
    return ds
end

# Modify Dataset
disallowmissing!(ds::Dataset, cols::MultiColumnIndex; error::Bool=true) =
    disallowmissing!(ds, index(ds)[cols], error=error)

# Modify Dataset
disallowmissing!(ds::Dataset, cols::Colon=:; error::Bool=true) =
    disallowmissing!(ds, axes(ds, 2), error=error)

"""
    repeat!(ds::Dataset; inner::Integer = 1, outer::Integer = 1)

Update a data set `ds` in-place by repeating its rows. `inner` specifies how many
times each row is repeated, and `outer` specifies how many times the full set
of rows is repeated. Columns of `ds` are freshly allocated.

# Example
```jldoctest
julia> ds = Dataset(a = 1:2, b = 3:4)
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> repeat!(ds, inner = 2, outer = 3);

julia> ds
12×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     1      3
   3 │     2      4
   4 │     2      4
   5 │     1      3
   6 │     1      3
   7 │     2      4
   8 │     2      4
   9 │     1      3
  10 │     1      3
  11 │     2      4
  12 │     2      4
```
"""
function repeat!(ds::Dataset; inner::Integer = 1, outer::Integer = 1)

# Modify Dataset
    inner <= 0 && throw(ArgumentError("inner keyword argument must be greater than zero"))
    outer <= 0 && throw(ArgumentError("outer keyword argument must be greater than zero"))
    if outer == 1
        for j in 1:ncol(ds)
            _columns(ds)[j] = repeat(_columns(ds)[j], inner = Int(inner), outer = 1)
        end
    elseif outer > 1
        for j in 1:ncol(ds)
            _columns(ds)[j] = repeat(_columns(ds)[j], inner = Int(inner), outer = Int(outer))
        end
        _reset_grouping_info!(ds)
    end
    _modified(_attributes(ds))
    ds
end

"""
    repeat!(ds::Dataset, count::Integer)

Update a data set `ds` in-place by repeating its rows the number of times
specified by `count`. Columns of `ds` are freshly allocated.

# Example
```jldoctest
julia> ds = Dataset(a = 1:2, b = 3:4)
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> repeat(ds, 2)
4×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4
   3 │     1      3
   4 │     2      4
```
"""
function repeat!(ds::Dataset, count::Integer)

# Modify Dataset
    count <= 0 && throw(ArgumentError("count must be greater than zero"))
    repeat!(ds, inner = 1, outer = count)
    ds
end

# This is not exactly copy! as in general we allow axes to be different

# Modify Dataset
function _replace_columns!(ds::Dataset, newds::Dataset)
    copy!(_columns(ds), _columns(newds))
    copy!(_names(index(ds)), _names(newds))
    copy!(index(ds).lookup, index(newds).lookup)
    copy!(index(ds).format, index(newds).format)
    _copy_grouping_info!(ds, newds)
    _modified(_attributes(ds))
    _attributes(ds).meta.info[] = _attributes(newds).meta.info[]
    # created date cannot be modified
    return ds
end


"""
    map(f::Function, ds::AbstractDataset, cols)
    map(f::Vector{Function}, ds::Dataset, cols)

Return a copy of `ds` where cols of the new `Dataset` is the result of calling `f` on each observation. The order of columns for the new data set is the same as `ds`.
Note that `map` guarantees not to reuse the columns from `ds` in the returned
`Dataset`. If `f` returns its argument then it gets copied before being stored.
The number of functions and the number of cols must match when multiple functions is used.

# Examples
```jldoctest
julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        11
   2 │        2        12
   3 │        3        13
   4 │        4        14

julia> map(x -> x^2, ds, :)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1       121
   2 │        4       144
   3 │        9       169
   4 │       16       196

julia> ds = Dataset(x = 1:10, y = repeat([1,2], inner = 5), c = 1:10)
10×3 Dataset
 Row │ x         y         c
     │ identity  identity  identity
     │ Int64     Int64     Int64
─────┼──────────────────────────────
   1 │        1         1         1
   2 │        2         1         2
   3 │        3         1         3
   4 │        4         1         4
   5 │        5         1         5
   6 │        6         2         6
   7 │        7         2         7
   8 │        8         2         8
   9 │        9         2         9
  10 │       10         2        10

julia> map([x -> x in (1,2,5), isequal(1)], ds[!, 1:2], :)
10×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Bool      Bool
─────┼────────────────────
   1 │     true      true
   2 │     true      true
   3 │    false      true
   4 │    false      true
   5 │     true      true
   6 │    false     false
   7 │    false     false
   8 │    false     false
   9 │    false     false
  10 │    false     false
```
"""
function Base.map(f::Function, ds::AbstractDataset, cols::MultiColumnIndex)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    transfer_grouping_info = !any(colsidx .∈ Ref(index(ds).sortedcols))
    sorted_colsidx = sort(colsidx)
    vs = AbstractVector[]
    for j in 1:ncol(ds)
        if insorted(j, sorted_colsidx)
            _f = f
        else
            _f = identity
        end
        v = _columns(ds)[j]
        fv = _f.(v)
        push!(vs, fv === v ? copy(fv) : fv)
    end
    if transfer_grouping_info
        newds_index = copy(index(ds))
    else
        newds_index = copy(index(ds))
        _reset_grouping_info!(newds_index)
    end
    # formats don't need to be transferred
    newds = Dataset(vs, newds_index, copycols=false)
    removeformat!(newds, cols)
    setinfo!(newds, _attributes(ds).meta.info[])
    return newds

end
Base.map(f::Union{Function, Type}, ds::AbstractDataset, col::ColumnIndex) = map(f, ds, [col])

function Base.map(f::Vector{Function}, ds::AbstractDataset, cols::MultiColumnIndex)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    @assert length(f) ==length(colsidx) "The number of functions and the number of columns must match"
    transfer_grouping_info = !any(colsidx .∈ Ref(index(ds).sortedcols))
    sorted_colsidx = sort(colsidx)
    counter_f = 1
    vs = AbstractVector[]
    for j in 1:ncol(ds)
        if insorted(j, sorted_colsidx)
            _f = f[counter_f]
            counter_f += 1
        else
            _f = identity
        end
        v = _columns(ds)[j]
        fv = _f.(v)
        push!(vs, fv === v ? copy(fv) : fv)
    end
    if transfer_grouping_info
        newds_index = copy(index(ds))
    else
        newds_index = copy(index(ds))
        _reset_grouping_info!(newds_index)
    end
    # formats don't need to be transferred
    newds = Dataset(vs, newds_index, copycols=false)
    removeformat!(newds, cols)
    setinfo!(newds, _attributes(ds).meta.info[])
    return newds

end

"""
    map!(f::Function, ds::Dataset, cols)

Update each `col` in `ds[!, cols]` in-place when `map!` return a result, and skip when it is not possible.

If `f` cannot be applied in place, use `map` for creating a copy of `ds`.

# Examples
```jldoctest
julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        11
   2 │        2        12
   3 │        3        13
   4 │        4        14

julia> map!(x -> x^2, ds, :);

julia> ds
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1       121
   2 │        4       144
   3 │        9       169
   4 │       16       196
```
"""
function Base.map!(f::Function, ds::AbstractDataset, cols::MultiColumnIndex)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    _reset_group = false
    # TODO needs function barrier
    number_of_warnnings = 0
    for j in 1:length(colsidx)
      try
        map!(f, _columns(ds)[colsidx[j]],  _columns(ds)[colsidx[j]])
        removeformat!(ds, colsidx[j])
        _modified(_attributes(ds))
        if !_reset_group && colsidx[j] ∈ index(ds).sortedcols
          _reset_grouping_info!(ds)
          _reset_group = true
        end
      catch
        if number_of_warnnings < 10
          @warn "cannot map `f` on ds[!, :$(_names(ds)[colsidx[j]])] in-place"
          number_of_warnnings += 1
        elseif number_of_warnnings == 10
          @warn "more than 10 columns can not be replaced ...."
          number_of_warnnings += 1
        end
        continue
      end
    end
    return ds
end

Base.map!(f::Union{Function, Type}, ds::AbstractDataset, col::ColumnIndex) = map!(f, ds, [col])

"""
    map!(f::Vector{Function}, ds::Dataset, cols)

Update jth `col` in `ds[!, cols]` in-place by mapping `f[j]` on it. If in-place mapping cannot be done, the mapping is skipped.

Use `map` if the in-place operation is not possible.

# Examples
```jldoctest
julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        11
   2 │        2        12
   3 │        3        13
   4 │        4        14

julia> map!([x -> x^2, x -> x-1], ds, :);

julia> ds
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        10
   2 │        4        11
   3 │        9        12
   4 │       16        13
```
"""
function Base.map!(f::Vector{Function}, ds::AbstractDataset, cols::MultiColumnIndex)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "The number of functions and the number of columns must match"
    _reset_group = false
    # TODO needs function barrier
    number_of_warnnings = 0
    for j in 1:length(colsidx)
      try
        map!(f[j], _columns(ds)[colsidx[j]],  _columns(ds)[colsidx[j]])
        removeformat!(ds, colsidx[j])
        _modified(_attributes(ds))
        if !_reset_group && colsidx[j] ∈ index(ds).sortedcols
          _reset_grouping_info!(ds)
          _reset_group = true
        end
      catch
        if number_of_warnnings < 10
          @warn "cannot map `f` on ds[!, :$(_names(ds)[colsidx[j]])] in-place"
          number_of_warnnings += 1
        elseif number_of_warnnings == 10
          @warn "more than 10 columns can not be replaced ...."
          number_of_warnnings += 1
        end
        continue
      end
    end
    return ds
end
