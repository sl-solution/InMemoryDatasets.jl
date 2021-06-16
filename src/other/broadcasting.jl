### Broadcasting

Base.getindex(ds::AbstractDataset, idx::CartesianIndex{2}) = ds[idx[1], idx[2]]
Base.view(ds::AbstractDataset, idx::CartesianIndex{2}) = view(ds, idx[1], idx[2])
Base.setindex!(ds::AbstractDataset, val, idx::CartesianIndex{2}) =
    (ds[idx[1], idx[2]] = val)

Base.broadcastable(ds::AbstractDataset) = ds

struct DatasetStyle <: Base.Broadcast.BroadcastStyle end

Base.Broadcast.BroadcastStyle(::Type{<:AbstractDataset}) =
    DatasetStyle()

Base.Broadcast.BroadcastStyle(::DatasetStyle, ::Base.Broadcast.BroadcastStyle) =
    DatasetStyle()
Base.Broadcast.BroadcastStyle(::Base.Broadcast.BroadcastStyle, ::DatasetStyle) =
    DatasetStyle()
Base.Broadcast.BroadcastStyle(::DatasetStyle, ::DatasetStyle) = DatasetStyle()

function copyto_widen!(res::AbstractVector{T}, bc::Base.Broadcast.Broadcasted,
                       pos, col) where T
    for i in pos:length(axes(bc)[1])
        val = bc[CartesianIndex(i, col)]
        S = typeof(val)
        if S <: T || promote_type(S, T) <: T
            res[i] = val
        else
            newres = similar(Vector{promote_type(S, T)}, length(res))
            copyto!(newres, 1, res, 1, i-1)
            newres[i] = val
            return copyto_widen!(newres, bc, i + 1, col)
        end
    end
    return res
end

function getcolbc(bcf::Base.Broadcast.Broadcasted{Style}, colind) where {Style}
    # we assume that bcf is already flattened and unaliased
    newargs = map(bcf.args) do x
        Base.Broadcast.extrude(x isa AbstractDataset ? x[!, colind] : x)
    end
    return Base.Broadcast.Broadcasted{Style}(bcf.f, newargs, bcf.axes)
end

function Base.copy(bc::Base.Broadcast.Broadcasted{DatasetStyle})
    ndim = length(axes(bc))
    if ndim != 2
        throw(DimensionMismatch("cannot broadcast a data frame into $ndim dimensions"))
    end
    bcf = Base.Broadcast.flatten(bc)
    colnames = unique!(Any[_names(ds) for ds in bcf.args if ds isa AbstractDataset])
    if length(colnames) != 1
        wrongnames = setdiff(union(colnames...), intersect(colnames...))
        if isempty(wrongnames)
            throw(ArgumentError("Column names in broadcasted data frames " *
                                "must have the same order"))
        else
            msg = join(wrongnames, ", ", " and ")
            throw(ArgumentError("Column names in broadcasted data frames must match. " *
                                "Non matching column names are $msg"))
        end
    end
    nrows = length(axes(bcf)[1])
    ds = Dataset()
    for i in axes(bcf)[2]
        if nrows == 0
            col = Any[]
        else
            bcf′ = getcolbc(bcf, i)
            v1 = bcf′[CartesianIndex(1, i)]
            startcol = similar(Vector{typeof(v1)}, nrows)
            startcol[1] = v1
            col = copyto_widen!(startcol, bcf′, 2, i)
        end
        ds[!, colnames[1][i]] = col
    end
    return ds
end

### Broadcasting assignment

struct LazyNewColDataset{T}
    ds::Dataset
    col::T
end

Base.axes(x::LazyNewColDataset) = (Base.OneTo(nrow(x.ds)),)
Base.ndims(::Type{<:LazyNewColDataset}) = 1

struct ColReplaceDataset
    ds::Dataset
    cols::Vector{Int}
end

Base.axes(x::ColReplaceDataset) = (axes(x.ds, 1), Base.OneTo(length(x.cols)))
Base.ndims(::Type{ColReplaceDataset}) = 2

Base.maybeview(ds::AbstractDataset, idx::CartesianIndex{2}) = ds[idx]
Base.maybeview(ds::AbstractDataset, row::Integer, col::ColumnIndex) = ds[row, col]
Base.maybeview(ds::AbstractDataset, rows, cols) = view(ds, rows, cols)

function Base.dotview(ds::Dataset, ::Colon, cols::ColumnIndex)
    haskey(index(ds), cols) && return view(ds, :, cols)
    if !(cols isa SymbolOrString)
        throw(ArgumentError("creating new columns using an integer index is disallowed"))
    end
    return LazyNewColDataset(ds, Symbol(cols))
end

function Base.dotview(ds::Dataset, ::typeof(!), cols)
    if !(cols isa ColumnIndex)
        return ColReplaceDataset(ds, index(ds)[cols])
    end
    if !(cols isa SymbolOrString) && cols > ncol(ds)
        throw(ArgumentError("creating new columns using an integer index is disallowed"))
    end
    return LazyNewColDataset(ds, cols isa AbstractString ? Symbol(cols) : cols)
end

# Base.dotview(ds::SubDataset, ::typeof(!), idxs) =
    # throw(ArgumentError("broadcasting with ! row selector is not allowed for SubDataset"))


# TODO: remove the deprecations when Julia 1.7 functionality is commonly used
#       by the community
if isdefined(Base, :dotgetproperty)
    function Base.dotgetproperty(ds::Dataset, col::SymbolOrString)
        if columnindex(ds, col) == 0
            return LazyNewColDataset(ds, Symbol(col))
        else
            Base.depwarn("In the future this operation will allocate a new column " *
                         "instead of performing an in-place assignment.", :dotgetproperty)
            return getproperty(ds, col)
        end
    end

    function Base.dotgetproperty(ds::SubDataset, col::SymbolOrString)
        Base.depwarn("broadcasting getproperty is deprecated for SubDataset and " *
                     "will be disallowed in  the future. Use `ds[:, $(repr(col))] .= ... instead",
                     :dotgetproperty)
        return getproperty(ds, col)
    end
end

function Base.copyto!(lazyds::LazyNewColDataset, bc::Base.Broadcast.Broadcasted{T}) where T
    if bc isa Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}}
        bc_tmp = Base.Broadcast.Broadcasted{T}(bc.f, bc.args, ())
        v = Base.Broadcast.materialize(bc_tmp)
        col = similar(Vector{typeof(v)}, nrow(lazyds.ds))
        copyto!(col, bc)
    else
        col = Base.Broadcast.materialize(bc)
    end
    lazyds.ds[!, lazyds.col] = col
end

function _copyto_helper!(dscol::AbstractVector, bc::Base.Broadcast.Broadcasted, col::Int)
    if axes(dscol, 1) != axes(bc)[1]
        # this should never happen unless data frame is corrupted (has unequal column lengths)
        throw(DimensionMismatch("Dimension mismatch in broadcasting. The updated" *
                                " data frame is invalid and should not be used"))
    end
    @inbounds for row in eachindex(dscol)
        dscol[row] = bc[CartesianIndex(row, col)]
    end
end

function Base.Broadcast.broadcast_unalias(dest::AbstractDataset, src)
    for col in eachcol(dest)
        src = Base.Broadcast.unalias(col, src)
    end
    return src
end

function Base.Broadcast.broadcast_unalias(dest, src::AbstractDataset)
    wascopied = false
    for (i, col) in enumerate(eachcol(src))
        if Base.mightalias(dest, col)
            if src isa SubDataset
                if !wascopied
                    src = SubDataset(copy(parent(src), copycols=false),
                                       index(src), rows(src))
                end
                parentidx = parentcols(index(src), i)
                parent(src)[!, parentidx] = Base.unaliascopy(parent(src)[!, parentidx])
            else
                if !wascopied
                    src = copy(src, copycols=false)
                end
                src[!, i] = Base.unaliascopy(col)
            end
            wascopied = true
        end
    end
    return src
end

function _broadcast_unalias_helper(dest::AbstractDataset, scol::AbstractVector,
                                   src::AbstractDataset, col2::Int, wascopied::Bool)
    # col1 can be checked till col2 point as we are writing broadcasting
    # results from 1 to ncol
    # we go downwards because aliasing when col1 == col2 is most probable
    for col1 in col2:-1:1
        dcol = dest[!, col1]
        if Base.mightalias(dcol, scol)
            if src isa SubDataset
                if !wascopied
                    src =SubDataset(copy(parent(src), copycols=false),
                                      index(src), rows(src))
                end
                parentidx = parentcols(index(src), col2)
                parent(src)[!, parentidx] = Base.unaliascopy(parent(src)[!, parentidx])
            else
                if !wascopied
                    src = copy(src, copycols=false)
                end
                src[!, col2] = Base.unaliascopy(scol)
            end
            return src, true
        end
    end
    return src, wascopied
end

function Base.Broadcast.broadcast_unalias(dest::AbstractDataset, src::AbstractDataset)
    if size(dest, 2) != size(src, 2)
        throw(DimensionMismatch("Dimension mismatch in broadcasting."))
    end
    wascopied = false
    for col2 in axes(dest, 2)
        scol = src[!, col2]
        src, wascopied = _broadcast_unalias_helper(dest, scol, src, col2, wascopied)
    end
    return src
end

function Base.copyto!(ds::AbstractDataset, bc::Base.Broadcast.Broadcasted)
    bcf = Base.Broadcast.flatten(bc)
    colnames = unique!(Any[_names(x) for x in bcf.args if x isa AbstractDataset])
    if length(colnames) > 1 || (length(colnames) == 1 && _names(ds) != colnames[1])
        push!(colnames, _names(ds))
        wrongnames = setdiff(union(colnames...), intersect(colnames...))
        if isempty(wrongnames)
            throw(ArgumentError("Column names in broadcasted data frames " *
                                "must have the same order"))
        else
            msg = join(wrongnames, ", ", " and ")
            throw(ArgumentError("Column names in broadcasted data frames must match. " *
                                "Non matching column names are $msg"))
        end
    end

    bcf′ = Base.Broadcast.preprocess(ds, bcf)
    for i in axes(ds, 2)
        _copyto_helper!(ds[!, i], getcolbc(bcf′, i), i)
    end
    return ds
end

function Base.copyto!(ds::AbstractDataset,
                      bc::Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}})
    # special case of fast approach when bc is providing an untransformed scalar
    if bc.f === identity && bc.args isa Tuple{Any} && Base.Broadcast.isflat(bc)
        for col in axes(ds, 2)
            fill!(ds[!, col], bc.args[1][])
        end
        return ds
    else
        return copyto!(ds, convert(Base.Broadcast.Broadcasted{Nothing}, bc))
    end
end

create_bc_tmp(bcf′_col::Base.Broadcast.Broadcasted{T}) where {T} =
    Base.Broadcast.Broadcasted{T}(bcf′_col.f, bcf′_col.args, ())

function Base.copyto!(crds::ColReplaceDataset, bc::Base.Broadcast.Broadcasted)
    bcf = Base.Broadcast.flatten(bc)
    colnames = unique!(Any[_names(x) for x in bcf.args if x isa AbstractDataset])
    if length(colnames) > 1 ||
        (length(colnames) == 1 && view(_names(crds.ds), crds.cols) != colnames[1])
        push!(colnames, view(_names(crds.ds), crds.cols))
        wrongnames = setdiff(union(colnames...), intersect(colnames...))
        if isempty(wrongnames)
            throw(ArgumentError("Column names in broadcasted data frames " *
                                "must have the same order"))
        else
            msg = join(wrongnames, ", ", " and ")
            throw(ArgumentError("Column names in broadcasted data frames must match. " *
                                "Non matching column names are $msg"))
        end
    end

    bcf′ = Base.Broadcast.preprocess(crds, bcf)
    nrows = length(axes(bcf′)[1])
    for (i, col_idx) in enumerate(crds.cols)
        bcf′_col = getcolbc(bcf′, i)
        if bcf′_col isa Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}}
            bc_tmp = create_bc_tmp(bcf′_col)
            v = Base.Broadcast.materialize(bc_tmp)
            newcol = similar(Vector{typeof(v)}, nrow(crds.ds))
            copyto!(newcol, bc)
        else
            if nrows == 0
                newcol = Any[]
            else
                v1 = bcf′_col[CartesianIndex(1, i)]
                startcol = similar(Vector{typeof(v1)}, nrows)
                startcol[1] = v1
                newcol = copyto_widen!(startcol, bcf′_col, 2, i)
            end
        end
        crds.ds[!, col_idx] = newcol
    end
    return crds.ds
end

Base.Broadcast.broadcast_unalias(dest::DatasetRow, src) =
    Base.Broadcast.broadcast_unalias(parent(dest), src)

function Base.copyto!(dsr::DatasetRow, bc::Base.Broadcast.Broadcasted)
    bc′ = Base.Broadcast.preprocess(dsr, bc)
    for I in eachindex(bc′)
        dsr[I] = bc′[I]
    end
    return dsr
end
