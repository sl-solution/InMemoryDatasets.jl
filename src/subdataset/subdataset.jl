"""
    SubDataset{<:Dataset, <:AbstractIndex, <:AbstractVector{Int}} <: Dataset

A view of a `Dataset`. It is returned by a call to the `view` function
on an `Dataset` if a collections of rows and columns are specified.

View of a data set preserves the `format` of columns.

# Examples
```jldoctest
julia> ds = Dataset(a = repeat([1, 2, 3, 4], outer=[2]),
                             b = repeat([2, 1], outer=[4]),
                             c = 1:8)
8×3 Dataset
 Row │ a         b         c
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         2         1
   2 │        2         1         2
   3 │        3         2         3
   4 │        4         1         4
   5 │        1         2         5
   6 │        2         1         6
   7 │        3         2         7
   8 │        4         1         8

julia> sds1 = view(ds, :, 2:3) # column subsetting
8×2 SubDataset
Row │ b         c
    │ identity  identity
    │ Int64?    Int64?
────┼────────────────────
  1 │        2         1
  2 │        1         2
  3 │        2         3
  4 │        1         4
  5 │        2         5
  6 │        1         6
  7 │        2         7
  8 │        1         8

julia> sds2 = @view ds[end:-1:1, [1, 3]]
8×2 SubDataset
Row │ a         c
    │ identity  identity
    │ Int64?    Int64?
────┼────────────────────
  1 │        4         8
  2 │        3         7
  3 │        2         6
  4 │        1         5
  5 │        4         4
  6 │        3         3
  7 │        2         2
  8 │        1         1

julia> setformat!(ds, 1=>iseven)
8×3 Dataset
 Row │ a       b         c
     │ iseven  identity  identity
     │ Int64?  Int64?    Int64?
─────┼────────────────────────────
   1 │  false         2         1
   2 │   true         1         2
   3 │  false         2         3
   4 │   true         1         4
   5 │  false         2         5
   6 │   true         1         6
   7 │  false         2         7
   8 │   true         1         8

julia> view(ds, 8:-1:1, [1,3])
8×2 SubDataset
 Row │ a       c
     │ iseven  identity
     │ Int64?  Int64?
─────┼──────────────────
   1 │   true         8
   2 │  false         7
   3 │   true         6
   4 │  false         5
   5 │   true         4
   6 │  false         3
   7 │   true         2
   8 │  false         1
```
"""
struct SubDataset{D<:AbstractDataset, S<:AbstractIndex, T<:AbstractVector{Int}} <: AbstractDataset
    parent::D
    colindex::S
    rows::T # maps from subds row indexes to parent row indexes
end

_attributes(sds::SubDataset) = getfield(parent(sds), :attributes)

# Experimental
function _columns(sds::SubDataset)
    allcols = AbstractArray[]
    colsidx = parentcols(index(sds))
    for j in 1:length(colsidx)
        push!(allcols, view(_columns(parent(sds))[colsidx[j]], rows(sds)))
    end
    allcols
end

Base.@propagate_inbounds function SubDataset(parent::Dataset, rows::AbstractVector{Int}, cols)
    @boundscheck if !checkindex(Bool, axes(parent, 1), rows)
        throw(BoundsError(parent, (rows, cols)))
    end
    SubDataset(parent, SubIndex(index(parent), cols), rows)
end
Base.@propagate_inbounds function SubDataset(parent::Dataset, rows::AbstractUnitRange, cols)
    @boundscheck if !checkindex(Bool, axes(parent, 1), rows)
        throw(BoundsError(parent, (rows, cols)))
    end
    SubDataset(parent, SubIndex(index(parent), cols), convert(Vector{Int}, rows))
end
Base.@propagate_inbounds SubDataset(parent::Dataset, ::Colon, cols) =
    SubDataset(parent, collect(axes(parent, 1)), cols)
@inline SubDataset(parent::Dataset, row::Integer, cols) =
    throw(ArgumentError("invalid row index: $row of type $(typeof(row))"))

Base.@propagate_inbounds function SubDataset(parent::Dataset, rows::AbstractVector{<:Integer}, cols)
    if any(x -> x isa Bool, rows)
        throw(ArgumentError("invalid row index of type `Bool`"))
    end
    return SubDataset(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds function SubDataset(parent::Dataset, rows::AbstractVector{Bool}, cols)
    if length(rows) != nrow(parent)
        throw(ArgumentError("invalid length of `AbstractVector{Bool}` row index " *
                            "(got $(length(rows)), expected $(nrow(parent)))"))
    end
    return SubDataset(parent, findall(rows), cols)
end

Base.@propagate_inbounds function SubDataset(parent::Dataset, rows::AbstractVector, cols)
    if !all(x -> (x isa Integer) && !(x isa Bool), rows)
        throw(ArgumentError("only `Integer` indices are accepted in `rows`"))
    end
    return SubDataset(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds SubDataset(sds::SubDataset, rowind, cols) =
    SubDataset(parent(sds), rows(sds)[rowind], parentcols(index(sds), cols))
Base.@propagate_inbounds SubDataset(sds::SubDataset, rowind::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

# TODO needs some extra work
# Base.@propagate_inbounds SubDataset(sds::SubDataset, rowind, ::Colon) =
    # if index(sds) isa Index # sds was created using : as row selector
    #     SubDataset(parent(sds), rows(sds)[rowind], :)
    # else
    #     SubDataset(parent(sds), rows(sds)[rowind], parentcols(index(sds), :))
    # end
Base.@propagate_inbounds SubDataset(sds::SubDataset, rowind::Bool, ::Colon) =
    throw(ArgumentError("invalid row index of type Bool"))
Base.@propagate_inbounds SubDataset(sds::SubDataset, ::Colon, cols) =
    SubDataset(parent(sds), rows(sds), parentcols(index(sds), cols))
@inline SubDataset(sds::SubDataset, ::Colon, ::Colon) = sds

# just for showing SubDataset
function _getformats_for_show(ds::SubDataset)
    res = Dict{Int, Function}()
    idx = index(ds)
    for i in 1:length(idx.cols)
        if haskey(idx.parent.format, idx.cols[i])
            push!(res, i => idx.parent.format[idx.cols[i]])
        end
    end
    res
end


rows(sds::SubDataset) = getfield(sds, :rows)
Base.parent(sds::SubDataset) = getfield(sds, :parent)
Base.parentindices(sds::SubDataset) = (rows(sds), parentcols(index(sds)))

function Base.view(ds::Dataset, rowinds, colind::ColumnIndex)
    idx = index(ds)[colind]
    SubDatasetColumn(idx, ds, _columns(ds)[idx], rowinds)
end

function Base.view(ds::Dataset, ::typeof(!), colind::ColumnIndex)
    idx = index(ds)[colind]
    SubDatasetColumn(idx, ds, _columns(ds)[idx], :)
end
function Base.view(dc::DatasetColumn, rowinds)
    SubDatasetColumn(dc.col, dc.ds, dc.val, rowinds)
end
function Base.view(sdc::SubDatasetColumn, rowinds)
    SubDatasetColumn(sdc.col, sdc.ds, sdc.val, sdc.selected_index[rowinds])
end


Base.@propagate_inbounds Base.view(ads::AbstractDataset, rowinds, colind::ColumnIndex) =
    view(ads[!, colind], rowinds)
Base.@propagate_inbounds Base.view(ads::AbstractDataset, ::typeof(!), colind::ColumnIndex) =
    view(ads[!, colind], :)

@inline Base.view(ads::AbstractDataset, rowinds, colind::Bool) =
    throw(ArgumentError("invalid column index $colind of type `Bool`"))
Base.@propagate_inbounds Base.view(ads::AbstractDataset, rowinds,
                                   colinds::MultiColumnIndex) =
    SubDataset(ads, rowinds, colinds)
Base.@propagate_inbounds Base.view(ads::AbstractDataset, rowinds::typeof(!),
                                   colinds::MultiColumnIndex) =
    SubDataset(ads, :, colinds)
Base.@propagate_inbounds Base.view(ads::AbstractDataset, rowinds::Not,
                                   colinds::MultiColumnIndex) =
    SubDataset(ads, axes(ads, 1)[rowinds], colinds)

##############################################################################
##
## AbstractDataset interface
##
##############################################################################

index(sds::SubDataset) = getfield(sds, :colindex)

nrow(sds::SubDataset) = ncol(sds) > 0 ? length(rows(sds))::Int : 0
ncol(sds::SubDataset) = length(index(sds))

Base.@propagate_inbounds Base.getindex(sds::SubDataset, rowind::Integer, colind::ColumnIndex) =
    parent(sds)[rows(sds)[rowind], parentcols(index(sds), colind)]
Base.@propagate_inbounds Base.getindex(sds::SubDataset, rowind::Bool, colind::ColumnIndex) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds Base.getindex(sds::SubDataset, rowinds::Union{AbstractVector, Not},
                                       colind::ColumnIndex; mapformats = false) =
    getindex(parent(sds), rows(sds)[rowinds], parentcols(index(sds), colind); mapformats = mapformats)
Base.@propagate_inbounds Base.getindex(sds::SubDataset, ::Colon, colind::ColumnIndex; mapformats = false) =
    getindex(parent(sds), rows(sds), parentcols(index(sds), colind); mapformats = mapformats)
Base.@propagate_inbounds Base.getindex(sds::SubDataset, ::typeof(!), colind::ColumnIndex) =
    view(parent(sds), rows(sds), parentcols(index(sds), colind))

Base.@propagate_inbounds Base.getindex(sds::SubDataset, rowinds::Union{AbstractVector, Not},
                                       colinds::MultiColumnIndex) =
    parent(sds)[rows(sds)[rowinds], parentcols(index(sds), colinds)]
Base.@propagate_inbounds Base.getindex(sds::SubDataset, ::Colon,
                                       colinds::MultiColumnIndex) =
    parent(sds)[rows(sds), parentcols(index(sds), colinds)]
Base.@propagate_inbounds Base.getindex(ds::SubDataset, row_ind::typeof(!),
                                       col_inds::MultiColumnIndex) =
    view(ds, :,  col_inds)


Base.@propagate_inbounds function Base.setindex!(sds::SubDataset, val::Any, idx::CartesianIndex{2})
    setindex!(sds, val, idx[1], idx[2])
end
Base.@propagate_inbounds function Base.setindex!(sds::SubDataset, val::Any, ::Colon, colinds::Any)
    parent(sds)[rows(sds), parentcols(index(sds), colinds)] = val
    return sds
end
Base.@propagate_inbounds function Base.setindex!(sds::SubDataset, val::Any, ::typeof(!), colinds::Any)
    throw(ArgumentError("setting index of SubDataset using ! as row selector is not allowed"))
end
Base.@propagate_inbounds function Base.setindex!(sds::SubDataset, val::Any, rowinds::Any, colinds::Any)
    parent(sds)[rows(sds)[rowinds], parentcols(index(sds), colinds)] = val
    return sds
end
Base.@propagate_inbounds Base.setindex!(sds::SubDataset, val::Any, rowinds::Bool, colinds::Any) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.setproperty!(::SubDataset, ::Symbol, ::Any) =
    throw(ArgumentError("Replacing or adding of columns of a SubDataset is not allowed. " *
                        "Instead use `ds[:, col_ind] = v` or `ds[:, col_ind] .= v` " *
                        "to perform an in-place assignment."))
Base.setproperty!(::SubDataset, ::AbstractString, ::Any) =
    throw(ArgumentError("Replacing or adding of columns of a SubDataset is not allowed. " *
                        "Instead use `ds[:, col_ind] = v` or `ds[:, col_ind] .= v` " *
                        "to perform an in-place assignment."))

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.copy(sds::SubDataset) = parent(sds)[rows(sds), parentcols(index(sds), :)]

# Base.deleteat!(ds::SubDataset, ind) =
    # throw(ArgumentError("SubDataset does not support deleting rows"))

function Dataset(sds::SubDataset)

    newds = sds[:, :]
    setinfo!(newds, _attributes(sds).meta.info[])
    newds
end

Base.convert(::Type{Dataset}, sds::SubDataset) = Dataset(sds)
