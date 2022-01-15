##############################################################################
##
## AbstractDataset interface
##
##############################################################################

index(ds::Dataset) = getfield(ds, :colindex)
_copy_grouping_info!(ds::Dataset, ds2::Dataset) = _copy_grouping_info!(index(ds), index(ds2))
_reset_grouping_info!(ds::Dataset) = _reset_grouping_info!(index(ds))
_attributes(ds::Dataset) = getfield(ds, :attributes)
# this function grants the access to the internal storage of columns of the
# `Dataset` and its use is unsafe. If the returned vector is mutated then
# make sure that:
# 1. `AbstractRange` columns are not added to a `Dataset`
# 2. all inserted columns use 1-based indexing
# 3. after several mutating operations on the vector are performed
#    each element (column) has the same length
# 4. if length of the vector is changed that the index of the `Dataset`
#    is adjusted appropriately

_onebased_check_error() =
    throw(ArgumentError("Currently InMemoryDatasets.jl supports only columns " *
                        "that use 1-based indexing"))
_onebased_check_error(i, col) =
    throw(ArgumentError("Currently InMemoryDatasets.jl supports only " *
                        "columns that use 1-based indexing, but " *
                        "column $i has starting index equal to $(firstindex(col))"))

# note: these type assertions are required to pass tests
nrow(ds::Dataset) = ncol(ds) > 0 ? length(_columns(ds)[1])::Int : 0
ncol(ds::Dataset) = length(index(ds))

##############################################################################
##
## Dataset consistency check
##
##############################################################################

corrupt_msg(ds::Dataset, i::Integer) =
    "Data set is corrupt: length of column " *
    ":$(_names(ds)[i]) ($(length(ds[!, i]))) " *
    "does not match length of column 1 ($(length(ds[!, 1]))). " *
    "The column vector has likely been resized unintentionally " *
    "(either directly or because it is shared with another data set)."

function _check_consistency(ds::Dataset)
    cols, idx = _columns(ds), index(ds)

    for (i, col) in enumerate(cols)
        firstindex(col) != 1 && _onebased_check_error(i, col)
    end

    ncols = length(cols)
    @assert length(idx.names) == length(idx.lookup) == ncols
    ncols == 0 && return nothing
    nrows = length(cols[1])
    for i in 2:length(cols)
        @assert length(cols[i]) == nrows corrupt_msg(ds, i)
    end
    nothing
end

function _check_consistency(ds::AbstractDataset)
    if ds isa SubDataset
        @assert length(index(ds).remap) == length(index(parent(ds))) "The parent data set which this view is based on, has been modified. To fix the issue recreate the view"
    end
    _check_consistency(parent(ds))
end

##############################################################################
##
## getindex() definitions
##
##############################################################################

# ds[SingleRowIndex, SingleColumnIndex] => Scalar
@inline function Base.getindex(ds::Dataset, row_ind::Integer,
                               col_ind::Union{Signed, Unsigned})
    cols = _columns(ds)
    @boundscheck begin
        if !checkindex(Bool, axes(cols, 1), col_ind)
            throw(BoundsError(ds, (row_ind, col_ind)))
        end
        if !checkindex(Bool, axes(ds, 1), row_ind)
            throw(BoundsError(ds, (row_ind, col_ind)))
        end
    end

    @inbounds cols[col_ind][row_ind]
end

@inline function Base.getindex(ds::Dataset, row_ind::Integer,
                               col_ind::SymbolOrString)
    selected_column = index(ds)[col_ind]
    @boundscheck if !checkindex(Bool, axes(ds, 1), row_ind)
        throw(BoundsError(ds, (row_ind, col_ind)))
    end
    @inbounds _columns(ds)[selected_column][row_ind]
end

_getindex(ds, r, c, f) = map(f, view(_columns(ds)[c], r))

# ds[MultiRowIndex, SingleColumnIndex] => AbstractVector, copy
@inline function Base.getindex(ds::Dataset, row_inds::AbstractVector, col_ind::ColumnIndex; mapformats = false)
    selected_column = index(ds)[col_ind]
    @boundscheck if !checkindex(Bool, axes(ds, 1), row_inds)
        throw(BoundsError(ds, (row_inds, col_ind)))
    end
    if mapformats
        _getindex(ds, row_inds, col_ind, getformat(ds, col_ind))
    else
        @inbounds return _columns(ds)[selected_column][row_inds]
    end
end

@inline function Base.getindex(ds::Dataset, row_inds::Not, col_ind::ColumnIndex; mapformats = false)
    if mapformats
        _getindex(ds, axes(ds, 1)[row_inds], col_ind, getformat(ds, col_ind))
    else
        ds[axes(ds, 1)[row_inds], col_ind]
    end
end


# ds[:, SingleColumnIndex] => AbstractVector
function Base.getindex(ds::Dataset, row_inds::Colon, col_ind::ColumnIndex; mapformats = false)
    selected_column = index(ds)[col_ind]
    if mapformats
        map(getformat(ds, col_ind), _columns(ds)[selected_column])
    else
        copy(_columns(ds)[selected_column])
    end
end

# ds[!, SingleColumnIndex] => AbstractVector, the same vector
@inline function Base.getindex(ds::Dataset, ::typeof(!), col_ind::Union{Signed, Unsigned})
    cols = _columns(ds)
    @boundscheck if !checkindex(Bool, axes(cols, 1), col_ind)
        throw(BoundsError(ds, (!, col_ind)))
    end
    @inbounds DatasetColumn(col_ind, ds, _columns(ds)[col_ind])
end

function Base.getindex(ds::Dataset, ::typeof(!), col_ind::SymbolOrString)
    selected_column = index(ds)[col_ind]
    ds[!, selected_column]
end

# ds[MultiRowIndex, MultiColumnIndex] => Dataset
function _threaded_permute(x, perm; threads = true)
    x_cpy = similar(x, 1)
    resize!(x_cpy, length(perm))
    @_threadsfor threads for i in 1:length(x_cpy)
        x_cpy[i] = x[perm[i]]
    end
    x_cpy
end

# Create Dataset
function _threaded_getindex(selected_rows::AbstractVector,
    selected_columns::AbstractVector,
    ds_columns::AbstractVector,
    idx::AbstractIndex)
    # FIXME threading should be done along rows rather than columns
    # @static if VERSION >= v"1.4"
    #     if length(selected_rows) >= 1_000_000 && Threads.nthreads() > 1
    #         new_columns = Vector{AbstractVector}(undef, length(selected_columns))
    #         @sync for i in eachindex(new_columns)
    #             Threads.@spawn new_columns[i] = ds_columns[selected_columns[i]][selected_rows]
    #         end
    #         return Dataset(new_columns, idx, copycols=false)
    #     else
    #         return Dataset(AbstractVector[ds_columns[i][selected_rows] for i in selected_columns],
    #                          idx, copycols=false)
    #     end
    # else
    #     return Dataset(AbstractVector[ds_columns[i][selected_rows] for i in selected_columns],
    #                      idx, copycols=false)
    # end
    new_columns = Vector{AbstractVector}(undef, length(selected_columns))
    # for many columns threads over columns
    if length(selected_columns) > 100
        Threads.@threads for j in 1:length(selected_columns)
            new_columns[j] = ds_columns[selected_columns[j]][selected_rows]
        end
    else
        for j in 1:length(selected_columns)
            # TODO should we be careful about types string, number, PooledArrays???
            if DataAPI.refpool(ds_columns[selected_columns[j]]) !== nothing
                pa = ds_columns[selected_columns[j]]
                if pa isa PooledArray
                    # we could use copy but it will be inefficient for small selected_rows
                    new_columns[j] = PooledArray(PooledArrays.RefArray(_threaded_permute(pa.refs, selected_rows)), DataAPI.invrefpool(pa), DataAPI.refpool(pa), PooledArrays.refcount(pa))
                else
                    # for other pooled data(like Categorical arrays) we don't have optimised path
                    new_columns[j] = pa[selected_rows]
                end
            else
                new_columns[j] = _threaded_permute(ds_columns[selected_columns[j]], selected_rows)
            end
        end
    end
    return Dataset(new_columns, idx, copycols=false)
end

# Create Dataset
@inline function Base.getindex(ds::Dataset, row_inds::AbstractVector{T},
                               col_inds::MultiColumnIndex) where T
    @boundscheck if !checkindex(Bool, axes(ds, 1), row_inds)
        throw(BoundsError(ds, (row_inds, col_inds)))
    end
    selected_columns = index(ds)[col_inds]

    u = _names(ds)[selected_columns]
    lookup = Dict{Symbol, Int}(zip(u, 1:length(u)))
    dsformat = index(ds).format
    format = Dict{Int, Function}()
    for i in 1:length(selected_columns)
      if haskey(dsformat, selected_columns[i])
        push!(format, i => dsformat[selected_columns[i]])
      end
    end
    # use this constructor to avoid checking twice if column names are not
    # duplicate as index(ds)[col_inds] already checks this
    idx = Index(lookup, u, format)

    if length(selected_columns) == 1
        newds = Dataset(AbstractVector[_columns(ds)[selected_columns[1]][row_inds]],
                         idx, copycols=false)
        setinfo!(newds, _attributes(ds).meta.info[])
        newds
    else
        # Computing integer indices once for all columns is faster
        selected_rows = T === Bool ? _findall(row_inds) : row_inds
        newds = _threaded_getindex(selected_rows, selected_columns, _columns(ds), idx)
        setinfo!(newds, _attributes(ds).meta.info[])
        newds
    end
end

# Create Dataset
@inline function Base.getindex(ds::Dataset, row_inds::AbstractVector{T}, ::Colon) where T
    @boundscheck if !checkindex(Bool, axes(ds, 1), row_inds)
        throw(BoundsError(ds, (row_inds, :)))
    end
    dsidx = index(ds)
    idx = Index(copy(dsidx.lookup), copy(dsidx.names), copy(dsidx.format))

    if ncol(ds) == 1
        newds = Dataset(AbstractVector[_columns(ds)[1][row_inds]], idx, copycols=false)
        setinfo!(newds, _attributes(ds).meta.info[])
        newds
    else
        # Computing integer indices once for all columns is faster
        selected_rows = T === Bool ? _findall(row_inds) : row_inds
        newds = _threaded_getindex(selected_rows, 1:ncol(ds), _columns(ds), idx)
        setinfo!(newds, _attributes(ds).meta.info[])
        newds
    end
end

# Create Dataset
@inline Base.getindex(ds::Dataset, row_inds::Not, col_inds::MultiColumnIndex) =
    ds[axes(ds, 1)[row_inds], col_inds]

# ds[:, MultiColumnIndex] => Dataset
# Create Dataset
# In select we should check if the attributes can be passed and also check how to set info
Base.getindex(ds::Dataset, row_ind::Colon, col_inds::MultiColumnIndex) =
    select(ds, col_inds)

# ds[!, MultiColumnIndex] => Dataset
# Create Dataset
# the same as : we should check about attributes which can be passed
function Base.getindex(ds::Dataset, row_ind::typeof(!), col_inds::MultiColumnIndex)
    colsidx = index(ds)[col_inds]
    view(ds, :, colsidx)
end
    # throw(ArgumentError("syntax `ds[!, col]` is only valid for single column, for multiple columns use `ds[:, cols]` or `select(ds, col...)`"))
