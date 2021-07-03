"""
    SubDataset{<:AbstractDataset, <:AbstractIndex, <:AbstractVector{Int}} <: AbstractDataset

A view of an `AbstractDataset`. It is returned by a call to the `view` function
on an `AbstractDataset` if a collections of rows and columns are specified.

A `SubDataset` is an `AbstractDataset`, so expect that most
Dataset functions should work. Such methods include `describe`,
`summary`, `nrow`, `size`, `by`, `stack`, and `join`.

If the selection of columns in a parent data frame is passed as `:` (a colon)
then `SubDataset` will always have all columns from the parent,
even if they are added or removed after its creation.

# Examples
```jldoctest
julia> df = Dataset(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8)
8×3 Dataset
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      1
   2 │     2      1      2
   3 │     3      2      3
   4 │     4      1      4
   5 │     1      2      5
   6 │     2      1      6
   7 │     3      2      7
   8 │     4      1      8

julia> sdf1 = view(df, :, 2:3) # column subsetting
8×2 SubDataset
 Row │ b      c
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     1      2
   3 │     2      3
   4 │     1      4
   5 │     2      5
   6 │     1      6
   7 │     2      7
   8 │     1      8

julia> sdf2 = @view df[end:-1:1, [1, 3]]  # row and column subsetting
8×2 SubDataset
 Row │ a      c
     │ Int64  Int64
─────┼──────────────
   1 │     4      8
   2 │     3      7
   3 │     2      6
   4 │     1      5
   5 │     4      4
   6 │     3      3
   7 │     2      2
   8 │     1      1

julia> sdf3 = groupby(df, :a)[1]  # indexing a GroupedDataset returns a SubDataset
2×3 SubDataset
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      1
   2 │     1      2      5
```
"""
struct SubDataset{D<:AbstractDataset, S<:AbstractIndex, T<:AbstractVector{Int}} <: AbstractDataset
    parent::D
    colindex::S
    rows::T # maps from subdf row indexes to parent row indexes
end

_attributes(sds::SubDataset) = getfield(parent(sds), :attributes)

# Experimental
function _columns(sds::SubDataset)
    allcols = AbstractArray[]
    for j in 1:ncol(parent(sds))
        push!(allcols, view(_columns(parent(sds))[j], rows(sds)))
    end
    allcols
end

Base.@propagate_inbounds function SubDataset(parent::Dataset, rows::AbstractVector{Int}, cols)
    @boundscheck if !checkindex(Bool, axes(parent, 1), rows)
        throw(BoundsError(parent, (rows, cols)))
    end
    SubDataset(parent, SubIndex(index(parent), cols), rows)
end
Base.@propagate_inbounds SubDataset(parent::Dataset, ::Colon, cols) =
    SubDataset(parent, axes(parent, 1), cols)
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
    return SubDataset(parent, _findall(rows), cols)
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


rows(sdf::SubDataset) = getfield(sdf, :rows)
Base.parent(sdf::SubDataset) = getfield(sdf, :parent)
Base.parentindices(sdf::SubDataset) = (rows(sdf), parentcols(index(sdf)))

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
    SubDataset(ads, axes(adf, 1)[rowinds], colinds)

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
                                       colind::ColumnIndex) =
    parent(sds)[rows(sds)[rowinds], parentcols(index(sds), colind)]
Base.@propagate_inbounds Base.getindex(sds::SubDataset, ::Colon, colind::ColumnIndex) =
    parent(sds)[rows(sds), parentcols(index(sds), colind)]
Base.@propagate_inbounds Base.getindex(sds::SubDataset, ::typeof(!), colind::ColumnIndex) =
    view(parent(sds), rows(sds), parentcols(index(sds), colind))

Base.@propagate_inbounds Base.getindex(sds::SubDataset, rowinds::Union{AbstractVector, Not},
                                       colinds::MultiColumnIndex) =
    parent(sds)[rows(sds)[rowinds], parentcols(index(sds), colinds)]
Base.@propagate_inbounds Base.getindex(sds::SubDataset, ::Colon,
                                       colinds::MultiColumnIndex) =
    parent(sds)[rows(sds), parentcols(index(sds), colinds)]
Base.@propagate_inbounds Base.getindex(df::SubDataset, row_ind::typeof(!),
                                       col_inds::MultiColumnIndex) =
    select(df, col_inds, copycols=false)


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
                        "Instead use `df[:, col_ind] = v` or `df[:, col_ind] .= v` " *
                        "to perform an in-place assignment."))
Base.setproperty!(::SubDataset, ::AbstractString, ::Any) =
    throw(ArgumentError("Replacing or adding of columns of a SubDataset is not allowed. " *
                        "Instead use `df[:, col_ind] = v` or `df[:, col_ind] .= v` " *
                        "to perform an in-place assignment."))

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.copy(sds::SubDataset) = parent(sds)[rows(sds), parentcols(index(sds), :)]

Base.delete!(ds::SubDataset, ind) =
    throw(ArgumentError("SubDataset does not support deleting rows"))

function Dataset(sds::SubDataset; copycols::Bool=true)
    if copycols
        sds[:, :]
    else
        newds = Dataset(collect(eachcol(sds)), Index(parent(index(sds)).lookup, parent(index(sds)).names, parent(index(sds)).format), copycols=false)
        setinfo!(newds, _attributes(sds).meta.info[])
        newds
    end
end

Base.convert(::Type{Dataset}, sds::SubDataset) = Dataset(sds)
