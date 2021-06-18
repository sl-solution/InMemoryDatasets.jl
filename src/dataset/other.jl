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
    mapcols(f::Function, ds::AbstractDataset, cols)

Return a copy of `ds` where cols of the new `Dataset` is the result of calling `f` on each observation. The order of columns for the new data set is the same as `ds`.
Note that `mapcols` guarantees not to reuse the columns from `ds` in the returned
`Dataset`. If `f` returns its argument then it gets copied before being stored.

# Examples
```jldoctest
julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> mapcols(x -> x.^2, ds, :)
4×2 Dataset
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1    121
   2 │     4    144
   3 │     9    169
   4 │    16    196
```
"""
function mapcols(f::Function, ds::AbstractDataset, cols::MultiColumnIndex)
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
mapcols(f::Union{Function, Type}, ds::AbstractDataset, col::ColumnIndex) = mapcols(f, ds, [col])


"""
    mapcols!(f::Function, ds::Dataset, cols)

Update each `col` in `ds[!, cols]` in-place for the columns that `map!(f, col, col)` works fine, update a copy of `col` by mapping `f` on ds[!, col], or just throw warnnings when `f` cannot be map on the elements of `ds[!, col]`. The order of columns for `ds` wouldn't change.

Note that `mapcols!` reuses the columns from `ds` if they are returned by `f`.

# Examples
```jldoctest
julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> mapcols!(x -> x.^2, ds, :);

julia> ds
4×2 Dataset
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1    121
   2 │     4    144
   3 │     9    169
   4 │    16    196
```
"""
function mapcols!(f::Function, ds::AbstractDataset, cols::MultiColumnIndex)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    transfer_grouping_info = !any(colsidx .∈ Ref(index(ds).sortedcols))
    if !transfer_grouping_info
        _reset_grouping_info!(ds)
    end
    sorted_colsidx = sort(colsidx)
    for j in 1:ncol(ds)
        T = eltype(_columns(ds)[j])
        if insorted(j, sorted_colsidx)
            try
                first_nonmissing = _columns(ds)[j][1]
                counter = 2
                while ismissing(first_nonmissing) && counter <= length(_columns(ds)[j])
                    first_nonmissing = _columns(ds)[j][counter]
                end
                if ismissing(first_nonmissing)
                    @warn "cannot map `f` on `ds[!, :$(_names(ds)[j])]`"
                    continue
                end
                # zeros to take care of Date
                S = typeof(f(first_nonmissing))
            catch
                @warn "cannot map `f` on `ds[!, :$(_names(ds)[j])]`"
                continue
            end
            if T >: Missing
                S = Union{S, Missing}
            end
            if promote_type(T, S) <: T
                map!(f, _columns(ds)[j],  _columns(ds)[j])
                removeformat!(ds, j)
                _modified(_attributes(ds))
            else
                _columns(ds)[j] = f.(_columns(ds)[j])
                removeformat!(ds, j)
                _modified(_attributes(ds))
            end
        end
    end
    return ds
end

mapcols!(f::Union{Function, Type}, ds::AbstractDataset, col::ColumnIndex) = mapcols!(f, ds, [col])
