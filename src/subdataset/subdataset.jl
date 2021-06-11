"""
    SubDataset{<:Abstractdataset, <:AbstractIndex, <:AbstractVector{Int}} <: Abstractdataset

A view of an `Abstractdataset`. It is returned by a call to the `view` function
on an `Abstractdataset` if a collections of rows and columns are specified.

A `SubDataset` is an `Abstractdataset`, so expect that most
dataset functions should work. Such methods include `describe`,
`summary`, `nrow`, `size`, `by`, `stack`, and `join`.

If the selection of columns in a parent data frame is passed as `:` (a colon)
then `SubDataset` will always have all columns from the parent,
even if they are added or removed after its creation.

# Examples
```jldoctest
julia> df = dataset(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8)
8×3 dataset
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

julia> sdf3 = groupby(df, :a)[1]  # indexing a Groupeddataset returns a SubDataset
2×3 SubDataset
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      1
   2 │     1      2      5
```
"""
struct SubDataset{D<:Abstractdataset, S<:AbstractIndex, T<:AbstractVector{Int}} <: Abstractdataset
    parent::D
    colindex::S
    rows::T # maps from subdf row indexes to parent row indexes
end

Base.@propagate_inbounds function SubDataset(parent::dataset, rows::AbstractVector{Int}, cols)
    @boundscheck if !checkindex(Bool, axes(parent, 1), rows)
        throw(BoundsError(parent, (rows, cols)))
    end
    SubDataset(parent, SubIndex(index(parent), cols), rows)
end
Base.@propagate_inbounds SubDataset(parent::dataset, ::Colon, cols) =
    SubDataset(parent, axes(parent, 1), cols)
@inline SubDataset(parent::dataset, row::Integer, cols) =
    throw(ArgumentError("invalid row index: $row of type $(typeof(row))"))

Base.@propagate_inbounds function SubDataset(parent::dataset, rows::AbstractVector{<:Integer}, cols)
    if any(x -> x isa Bool, rows)
        throw(ArgumentError("invalid row index of type `Bool`"))
    end
    return SubDataset(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds function SubDataset(parent::dataset, rows::AbstractVector{Bool}, cols)
    if length(rows) != nrow(parent)
        throw(ArgumentError("invalid length of `AbstractVector{Bool}` row index " *
                            "(got $(length(rows)), expected $(nrow(parent)))"))
    end
    return SubDataset(parent, _findall(rows), cols)
end

Base.@propagate_inbounds function SubDataset(parent::dataset, rows::AbstractVector, cols)
    if !all(x -> (x isa Integer) && !(x isa Bool), rows)
        throw(ArgumentError("only `Integer` indices are accepted in `rows`"))
    end
    return SubDataset(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds SubDataset(sdf::SubDataset, rowind, cols) =
    SubDataset(parent(sdf), rows(sdf)[rowind], parentcols(index(sdf), cols))
Base.@propagate_inbounds SubDataset(sdf::SubDataset, rowind::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))
Base.@propagate_inbounds SubDataset(sdf::SubDataset, rowind, ::Colon) =
    if index(sdf) isa Index # sdf was created using : as row selector
        SubDataset(parent(sdf), rows(sdf)[rowind], :)
    else
        SubDataset(parent(sdf), rows(sdf)[rowind], parentcols(index(sdf), :))
    end
Base.@propagate_inbounds SubDataset(sdf::SubDataset, rowind::Bool, ::Colon) =
    throw(ArgumentError("invalid row index of type Bool"))
Base.@propagate_inbounds SubDataset(sdf::SubDataset, ::Colon, cols) =
    SubDataset(parent(sdf), rows(sdf), parentcols(index(sdf), cols))
@inline SubDataset(sdf::SubDataset, ::Colon, ::Colon) = sdf

rows(sdf::SubDataset) = getfield(sdf, :rows)
Base.parent(sdf::SubDataset) = getfield(sdf, :parent)
Base.parentindices(sdf::SubDataset) = (rows(sdf), parentcols(index(sdf)))

Base.@propagate_inbounds Base.view(adf::Abstractdataset, rowinds, colind::ColumnIndex) =
    view(adf[!, colind], rowinds)
Base.@propagate_inbounds Base.view(adf::Abstractdataset, ::typeof(!), colind::ColumnIndex) =
    view(adf[!, colind], :)
@inline Base.view(adf::Abstractdataset, rowinds, colind::Bool) =
    throw(ArgumentError("invalid column index $colind of type `Bool`"))
Base.@propagate_inbounds Base.view(adf::Abstractdataset, rowinds,
                                   colinds::MultiColumnIndex) =
    SubDataset(adf, rowinds, colinds)
Base.@propagate_inbounds Base.view(adf::Abstractdataset, rowinds::typeof(!),
                                   colinds::MultiColumnIndex) =
    SubDataset(adf, :, colinds)
Base.@propagate_inbounds Base.view(adf::Abstractdataset, rowinds::Not,
                                   colinds::MultiColumnIndex) =
    SubDataset(adf, axes(adf, 1)[rowinds], colinds)

##############################################################################
##
## Abstractdataset interface
##
##############################################################################

index(sdf::SubDataset) = getfield(sdf, :colindex)

nrow(sdf::SubDataset) = ncol(sdf) > 0 ? length(rows(sdf))::Int : 0
ncol(sdf::SubDataset) = length(index(sdf))

Base.@propagate_inbounds Base.getindex(sdf::SubDataset, rowind::Integer, colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowind], parentcols(index(sdf), colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataset, rowind::Bool, colind::ColumnIndex) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds Base.getindex(sdf::SubDataset, rowinds::Union{AbstractVector, Not},
                                       colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowinds], parentcols(index(sdf), colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataset, ::Colon, colind::ColumnIndex) =
    parent(sdf)[rows(sdf), parentcols(index(sdf), colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataset, ::typeof(!), colind::ColumnIndex) =
    view(parent(sdf), rows(sdf), parentcols(index(sdf), colind))

Base.@propagate_inbounds Base.getindex(sdf::SubDataset, rowinds::Union{AbstractVector, Not},
                                       colinds::MultiColumnIndex) =
    parent(sdf)[rows(sdf)[rowinds], parentcols(index(sdf), colinds)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataset, ::Colon,
                                       colinds::MultiColumnIndex) =
    parent(sdf)[rows(sdf), parentcols(index(sdf), colinds)]
Base.@propagate_inbounds Base.getindex(df::SubDataset, row_ind::typeof(!),
                                       col_inds::MultiColumnIndex) =
    select(df, col_inds, copycols=false)


Base.@propagate_inbounds function Base.setindex!(sdf::SubDataset, val::Any, idx::CartesianIndex{2})
    setindex!(sdf, val, idx[1], idx[2])
end
Base.@propagate_inbounds function Base.setindex!(sdf::SubDataset, val::Any, ::Colon, colinds::Any)
    parent(sdf)[rows(sdf), parentcols(index(sdf), colinds)] = val
    return sdf
end
Base.@propagate_inbounds function Base.setindex!(sdf::SubDataset, val::Any, ::typeof(!), colinds::Any)
    throw(ArgumentError("setting index of SubDataset using ! as row selector is not allowed"))
end
Base.@propagate_inbounds function Base.setindex!(sdf::SubDataset, val::Any, rowinds::Any, colinds::Any)
    parent(sdf)[rows(sdf)[rowinds], parentcols(index(sdf), colinds)] = val
    return sdf
end
Base.@propagate_inbounds Base.setindex!(sdf::SubDataset, val::Any, rowinds::Bool, colinds::Any) =
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

Base.copy(sdf::SubDataset) = parent(sdf)[rows(sdf), parentcols(index(sdf), :)]

Base.delete!(df::SubDataset, ind) =
    throw(ArgumentError("SubDataset does not support deleting rows"))

function dataset(sdf::SubDataset; copycols::Bool=true)
    if copycols
        sdf[:, :]
    else
        dataset(collect(eachcol(sdf)), _names(sdf), copycols=false)
    end
end

Base.convert(::Type{dataset}, sdf::SubDataset) = dataset(sdf)
