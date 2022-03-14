"""
    DatasetRow{<:AbstractDataset, <:AbstractIndex}

A view of one row of an `AbstractDataset`.

A `DatasetRow` is returned by `getindex` or `view` functions when one row and a
selection of columns are requested, or when iterating the result
of the call to the [`eachrow`](@ref) function.

The `DatasetRow` constructor can also be called directly:

```
DatasetRow(parent::AbstractDataset, row::Integer, cols=:)
```

A `DatasetRow` supports the iteration interface and can therefore be passed to
functions that expect a collection as an argument. Its element type is always `Any`.

Indexing is one-dimensional like specifying a column of a `Dataset`.
You can also access the data in a `DatasetRow` using the `getproperty` and
`setproperty!` functions and convert it to a `Tuple`, `NamedTuple`, or `Vector`
using the corresponding functions.

If the selection of columns in a parent data frame is passed as `:` (a colon)
then `DatasetRow` will always have all columns from the parent,
even if they are added or removed after its creation.

# Examples
```jldoctest
julia> ds = Dataset(a = repeat([1, 2], outer=[2]),
                      b = repeat(["a", "b"], inner=[2]),
                      c = 1:4)
4×3 Dataset
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1
   2 │     2  a           2
   3 │     1  b           3
   4 │     2  b           4

julia> ds[1, :]
DatasetRow
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1

julia> @view ds[end, [:a]]
DatasetRow
 Row │ a
     │ Int64
─────┼───────
   4 │     2

julia> eachrow(ds)[1]
DatasetRow
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1

julia> Tuple(ds[1, :])
(1, "a", 1)

julia> NamedTuple(ds[1, :])
(a = 1, b = "a", c = 1)

julia> Vector(ds[1, :])
3-element Vector{Any}:
 1
  "a"
 1
```
"""
struct DatasetRow{D<:AbstractDataset, S<:AbstractIndex}
    # although we allow D to be AbstractDataset to support extensions
    # in Dataset.jl it will always be a Dataset unless an inner constructor
    # is used. In this way we have a fast access to the data frame that
    # actually stores the data that DatasetRow refers to
    ds::D
    colindex::S
    dsrow::Int # row number in ds
    rownumber::Int # row number in the direct source AbstractDataset from which DatasetRow was created

    @inline DatasetRow(ds::D, colindex::S, row::Union{Signed, Unsigned},
                         rownumber::Union{Signed, Unsigned}) where
        {D<:AbstractDataset, S<:AbstractIndex} = new{D, S}(ds, colindex, row, rownumber)
end

_getnames(x::DatasetRow) = _names(x)

Base.@propagate_inbounds function DatasetRow(ds::Dataset, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(ds, 1), row)
        throw(BoundsError(ds, (row, cols)))
    end
    DatasetRow(ds, SubIndex(index(ds), cols), row, row)
end

Base.@propagate_inbounds DatasetRow(ds::Dataset, row::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds function DatasetRow(sds::SubDataset, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(sds, 1), row)
        throw(BoundsError(sds, (row, cols)))
    end
    if index(sds) isa Index # sds was created using : as row selector
        colindex = SubIndex(index(sds), cols)
    else
        colindex = SubIndex(index(parent(sds)), parentcols(index(sds), cols))
    end
    @inbounds DatasetRow(parent(sds), colindex, rows(sds)[row], row)
end

Base.@propagate_inbounds DatasetRow(ds::SubDataset, row::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds DatasetRow(ds::AbstractDataset, row::Integer) =
    DatasetRow(ds, row, :)

row(r::DatasetRow) = getfield(r, :dsrow)

"""
    rownumber(dsr::DatasetRow)

Return a row number in the `AbstractDataset` that `dsr` was created from.

Note that this differs from the first element in the tuple returned by
`parentindices`. The latter gives the row number in the `parent(dsr)`, which is
the source `Dataset` where data that `dsr` gives access to is stored.

# Examples
```jldoctest
julia> ds = Dataset(reshape(1:12, 3, 4), :auto)
3×4 Dataset
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12

julia> dsr = ds[2, :]
DatasetRow
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   2 │     2      5      8     11

julia> rownumber(dsr)
2

julia> parentindices(dsr)
(2, Base.OneTo(4))

julia> parent(dsr)
3×4 Dataset
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12

julia> dsv = @view ds[2:3, 1:3]
2×3 SubDataset
 Row │ x1     x2     x3
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      5      8
   2 │     3      6      9

julia> dsrv = dsv[2, :]
DatasetRow
 Row │ x1     x2     x3
     │ Int64  Int64  Int64
─────┼─────────────────────
   3 │     3      6      9

julia> rownumber(dsrv)
2

julia> parentindices(dsrv)
(3, 1:3)

julia> parent(dsrv)
3×4 Dataset
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12
```
"""
rownumber(r::DatasetRow) = getfield(r, :rownumber)

Base.parent(r::DatasetRow) = getfield(r, :ds)
Base.parentindices(r::DatasetRow) = (row(r), parentcols(index(r)))

Base.summary(dsr::DatasetRow) = # -> String
    @sprintf("%d-element %s", length(dsr), nameof(typeof(dsr)))
Base.summary(io::IO, dsr::DatasetRow) = print(io, summary(dsr))

Base.@propagate_inbounds Base.view(ads::AbstractDataset, rowind::Integer,
                                   colinds::MultiColumnIndex) =
    DatasetRow(ads, rowind, colinds)

Base.@propagate_inbounds Base.getindex(ds::AbstractDataset, rowind::Integer,
                                       colinds::MultiColumnIndex) =
    DatasetRow(ds, rowind, colinds)
Base.@propagate_inbounds Base.getindex(ds::AbstractDataset, rowind::Integer, ::Colon) =
    DatasetRow(ds, rowind, :)
Base.@propagate_inbounds Base.getindex(r::DatasetRow, idx::ColumnIndex) =
    parent(r)[row(r), parentcols(index(r), idx)]

Base.@propagate_inbounds function Base.getindex(r::DatasetRow, idxs::MultiColumnIndex)
    # we create a temporary DatasetRow object to compute the SubIndex
    # in the parent(r), but this object has an incorrect rownumber
    # so we later copy rownumber from r
    # the Julia compiler should be able to optimize out this indirection
    # and in this way we avoid duplicating the code that computes the correct SubIndex
    dsr_tmp = DatasetRow(parent(r), row(r), parentcols(index(r), idxs))
    return DatasetRow(parent(dsr_tmp), index(dsr_tmp), row(r), rownumber(r))
end

Base.@propagate_inbounds Base.getindex(r::DatasetRow, ::Colon) = r

# Modify Dataset
for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  v::Union{DatasetRow, NamedTuple, AbstractDict},
                                  row_ind::Integer,
                                  col_inds::$(T))
        idxs = index(ds)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned " *
                                    "collection contains $(length(v)) elements"))
        end

        if v isa AbstractDict
            if keytype(v) !== Symbol &&
                (keytype(v) <: AbstractString || all(x -> x isa AbstractString, keys(v)))
                v = (;(Symbol.(keys(v)) .=> values(v))...)
            end
            for n in view(_names(ds), idxs)
                if !haskey(v, n)
                    throw(ArgumentError("Column :$n not found in source dictionary"))
                end
            end
        elseif !all(((a, b),) -> a == b, zip(view(_names(ds), idxs), keys(v)))
            mismatched = findall(view(_names(ds), idxs) .!= collect(keys(v)))
            throw(ArgumentError("Selected column names do not match the names in assigned " *
                                "value in positions $(join(mismatched, ", ", " and "))"))
        end
        flag = false
        for (col, val) in pairs(v)
            ds[row_ind, col] = val
            if !flag && index(ds)[col] ∈ index(ds).sortedcols
                _reset_grouping_info!(ds)
                flag = true
            end
        end
        _modified(_attributes(ds))
        return ds
    end
end

Base.@propagate_inbounds Base.setindex!(r::DatasetRow, value, idx) =
    setindex!(parent(r), value, row(r), parentcols(index(r), idx))

index(r::DatasetRow) = getfield(r, :colindex)

Base.names(r::DatasetRow, cols::Colon=:) = names(index(r))

function Base.names(r::DatasetRow, cols)
    nms = _names(index(r))
    idx = index(r)[cols]
    idxs = idx isa Int ? (idx:idx) : idx
    return [string(nms[i]) for i in idxs]
end

Base.names(r::DatasetRow, T::Type) =
    [String(n) for n in _names(r) if eltype(parent(r)[!, n]) <: T]
Base.names(r::DatasetRow, fun::Function) = filter!(fun, names(r))

_names(r::DatasetRow) = view(_names(parent(r)), parentcols(index(r), :))

Base.haskey(r::DatasetRow, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.haskey(r::DatasetRow, key::Integer) = 1 ≤ key ≤ size(r, 1)

function Base.haskey(r::DatasetRow, key::Symbol)
    hasproperty(parent(r), key) || return false
    index(r) isa Index && return true
    # here index(r) is a SubIndex
    pos = index(parent(r))[key]
    remap = index(r).remap
    length(remap) == 0 && lazyremap!(index(r))
    checkbounds(Bool, remap, pos) || return false
    return remap[pos] > 0
end

Base.haskey(r::DatasetRow, key::AbstractString) = haskey(r, Symbol(key))

# separate methods are needed due to dispatch ambiguity
Base.getproperty(r::DatasetRow, idx::Symbol) = r[idx]
Base.getproperty(r::DatasetRow, idx::AbstractString) = r[idx]
Base.setproperty!(r::DatasetRow, idx::Symbol, x::Any) = (r[idx] = x)
Base.setproperty!(r::DatasetRow, idx::AbstractString, x::Any) = (r[idx] = x)
Compat.hasproperty(r::DatasetRow, s::Symbol) = haskey(index(r), s)
Compat.hasproperty(r::DatasetRow, s::AbstractString) = haskey(index(r), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(r::DatasetRow, private::Bool=false) = copy(_names(r))

Base.view(r::DatasetRow, col::ColumnIndex) =
    view(parent(r)[!, parentcols(index(r), col)], row(r))

function Base.view(r::DatasetRow, cols::MultiColumnIndex)
    # we create a temporary DatasetRow object to compute the SubIndex
    # in the parent(r), but this object has an incorrect rownumber
    # so we later copy rownumber from r
    # the Julia compiler should be able to optimize out this indirection
    # and in this way we avoid duplicating the code that computes the correct SubIndex
    dsr_tmp = DatasetRow(parent(r), row(r), parentcols(index(r), cols))
    return DatasetRow(parent(dsr_tmp), index(dsr_tmp), row(r), rownumber(r))
end

Base.view(r::DatasetRow, ::Colon) = r

"""
    size(dsr::DatasetRow[, dim])

Return a 1-tuple containing the number of elements of `dsr`.
If an optional dimension `dim` is specified, it must be `1`, and the number of
elements is returned directly as a number.

See also: [`length`](@ref)

# Examples
```jldoctest
julia> dsr = Dataset(a=1:3, b='a':'c')[1, :]
DatasetRow
 Row │ a      b
     │ Int64  Char
─────┼─────────────
   1 │     1  a

julia> size(dsr)
(2,)

julia> size(dsr, 1)
2
```
"""
Base.size(r::DatasetRow) = (length(index(r)),)
Base.size(r::DatasetRow, i) = size(r)[i]

"""
    length(dsr::DatasetRow)

Return the number of elements of `dsr`.

See also: [`size`](@ref)

# Examples
```jldoctest
julia> dsr = Dataset(a=1:3, b='a':'c')[1, :]
DatasetRow
 Row │ a      b
     │ Int64  Char
─────┼─────────────
   1 │     1  a

julia> length(dsr)
2
```
"""
Base.length(r::DatasetRow) = size(r, 1)

"""
    ndims(::DatasetRow)
    ndims(::Type{<:DatasetRow})

Return the number of dimensions of a data frame row, which is always `1`.
"""
Base.ndims(::DatasetRow) = 1
Base.ndims(::Type{<:DatasetRow}) = 1

Base.firstindex(r::DatasetRow) = 1
Base.lastindex(r::DatasetRow) = length(r)

if VERSION < v"1.6"
    Base.firstindex(r::DatasetRow, i::Integer) = first(axes(r, i))
    Base.lastindex(r::DatasetRow, i::Integer) = last(axes(r, i))
end
Base.axes(r::DatasetRow, i::Integer) = Base.OneTo(size(r, i))

Base.iterate(r::DatasetRow) = iterate(r, 1)

function Base.iterate(r::DatasetRow, st)
    st > length(r) && return nothing
    return (r[st], st + 1)
end

# Computing the element type requires going over all columns,
# so better let collect() do it only if necessary (widening)
Base.IteratorEltype(::Type{<:DatasetRow}) = Base.EltypeUnknown()

function Base.Vector(dsr::DatasetRow)
    ds = parent(dsr)
    T = reduce(promote_type, (eltype(ds[!, i]) for i in parentcols(index(dsr))))
    return Vector{T}(dsr)
end
Base.Vector{T}(dsr::DatasetRow) where T =
    T[dsr[i] for i in 1:length(dsr)]

Base.Array(dsr::DatasetRow) = Vector(dsr)
Base.Array{T}(dsr::DatasetRow) where {T} = Vector{T}(dsr)

Base.keys(r::DatasetRow) = propertynames(r)
Base.values(r::DatasetRow) =
    ntuple(col -> parent(r)[row(r), parentcols(index(r), col)], length(r))
Base.map(f, r::DatasetRow, rs::DatasetRow...) = map(f, copy(r), copy.(rs)...)
Base.get(dsr::DatasetRow, key::ColumnIndex, default) =
    haskey(dsr, key) ? dsr[key] : default
Base.get(f::Base.Callable, dsr::DatasetRow, key::ColumnIndex) =
    haskey(dsr, key) ? dsr[key] : f()
Base.broadcastable(::DatasetRow) =
    throw(ArgumentError("broadcasting over `DatasetRow`s is reserved"))

function Base.NamedTuple(dsr::DatasetRow)
    k = Tuple(_names(dsr))
    v = ntuple(i -> dsr[i], length(dsr))
    pc = parentcols(index(dsr))
    cols = _columns(parent(dsr))
    s = ntuple(i -> eltype(cols[pc[i]]), length(dsr))
    NamedTuple{k, Tuple{s...}}(v)
end

"""
    copy(dsr::DatasetRow)

Construct a `NamedTuple` with the same contents as the [`DatasetRow`](@ref).
This method returns a `NamedTuple` so that the returned object
is not affected by changes to the parent data frame of which `dsr` is a view.

"""
Base.copy(dsr::DatasetRow) = NamedTuple(dsr)

Base.convert(::Type{NamedTuple}, dsr::DatasetRow) = NamedTuple(dsr)

Base.merge(a::DatasetRow) = NamedTuple(a)
Base.merge(a::DatasetRow, b::NamedTuple) = merge(NamedTuple(a), b)
Base.merge(a::NamedTuple, b::DatasetRow) = merge(a, NamedTuple(b))
Base.merge(a::DatasetRow, b::DatasetRow) = merge(NamedTuple(a), NamedTuple(b))
Base.merge(a::DatasetRow, b::Base.Iterators.Pairs) = merge(NamedTuple(a), b)
Base.merge(a::DatasetRow, itr) = merge(NamedTuple(a), itr)

Base.hash(r::DatasetRow, h::UInt) = _nt_like_hash(r, h)

for eqfun in (:isequal, :(==)),
    (leftarg, rightarg) in ((:DatasetRow, :DatasetRow),
                            (:DatasetRow, :NamedTuple),
                            (:NamedTuple, :DatasetRow))
    @eval function Base.$eqfun(r1::$leftarg, r2::$rightarg)
        _equal_names(r1, r2) || return false
        return all(((a, b),) -> $eqfun(a, b), zip(r1, r2))
    end
end

for (eqfun, cmpfun) in ((:isequal, :isless), (:(==), :(<))),
    (leftarg, rightarg) in ((:DatasetRow, :DatasetRow),
                            (:DatasetRow, :NamedTuple),
                            (:NamedTuple, :DatasetRow))
    @eval function Base.$cmpfun(r1::$leftarg, r2::$rightarg)
        if !_equal_names(r1, r2)
            length(r1) == length(r2) ||
                throw(ArgumentError("compared objects must have the same number " *
                                    "of columns (got $(length(r1)) and $(length(r2)))"))
            mismatch = findfirst(i -> _getnames(r1)[i] != _getnames(r2)[i], 1:length(r1))
            throw(ArgumentError("compared objects must have the same property " *
                                "names but they differ in column number $mismatch " *
                                "where the names are :$(_getnames(r1)[mismatch]) and " *
                                ":$(_getnames(r2)[mismatch]) respectively"))
        end
        for (a, b) in zip(r1, r2)
            eq = $eqfun(a, b)
            if ismissing(eq)
                return missing
            elseif !eq
                return $cmpfun(a, b)
            end
        end
        return false # here we know that r1 and r2 have equal lengths and all values were equal
    end
end

function Dataset(dsr::DatasetRow)
    row, cols = parentindices(dsr)
    parent(dsr)[row:row, cols]
end

Base.push!(ds::Dataset, dsr::DatasetRow, cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset])) =
    push!(ds, NamedTuple(dsr); cols = cols, promote = promote)
Base.pushfirst!(ds::Dataset, dsr::DatasetRow, cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset])) =
    pushfirst!(ds, NamedTuple(dsr); cols = cols, promote = promote)
# @noinline pushhelper!(x, r) = push!(x, x[r])
#
# function Base.push!(ds::Dataset, dsr::DatasetRow; cols::Symbol=:setequal,
#                     promote::Bool=(cols in [:union, :subset]))
#     possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
#     if !(cols in possible_cols)
#         throw(ArgumentError("`cols` keyword argument must be any of :" *
#                             join(possible_cols, ", :")))
#     end
#
#     nrows, ncols = size(ds)
#     targetrows = nrows + 1
#
#     if parent(dsr) === ds && index(dsr) isa Index
#         # in this case we are sure that all we do is safe
#         r = row(dsr)
#         for col in _columns(ds)
#             # use a barrier function to improve performance
#             pushhelper!(col, r)
#         end
#         for (colname, col) in zip(_names(ds), _columns(ds))
#             if length(col) != targetrows
#                 for col2 in _columns(ds)
#                     resize!(col2, nrows)
#                 end
#                 throw(AssertionError("Error adding value to column :$colname"))
#             end
#         end
#         return ds
#     end
#
#     if ncols == 0
#         for (n, v) in pairs(dsr)
#             setproperty!(ds, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
#         end
#         return ds
#     end
#
#     if cols == :union
#         for (i, colname) in enumerate(_names(ds))
#             col = _columns(ds)[i]
#             if hasproperty(dsr, colname)
#                 val = dsr[colname]
#             else
#                 val = missing
#             end
#             S = typeof(val)
#             T = eltype(col)
#             if S <: T || promote_type(S, T) <: T
#                 push!(col, val)
#             elseif !promote
#                 try
#                     push!(col, val)
#                 catch err
#                     for col in _columns(ds)
#                         resize!(col, nrows)
#                     end
#                     @error "Error adding value to column :$colname."
#                     rethrow(err)
#                 end
#             else
#                 newcol = Tables.allocatecolumn(promote_type(S, T), targetrows)
#                 copyto!(newcol, 1, col, 1, nrows)
#                 newcol[end] = val
#                 firstindex(newcol) != 1 && _onebased_check_error()
#                 _columns(ds)[i] = newcol
#             end
#         end
#         for (colname, col) in zip(_names(ds), _columns(ds))
#             if length(col) != targetrows
#                 for col2 in _columns(ds)
#                     resize!(col2, nrows)
#                 end
#                 throw(AssertionError("Error adding value to column :$colname"))
#             end
#         end
#         for colname in setdiff(_names(dsr), _names(ds))
#             val = dsr[colname]
#             S = typeof(val)
#             if nrows == 0
#                 newcol = [val]
#             else
#                 newcol = Tables.allocatecolumn(Union{Missing, S}, targetrows)
#                 fill!(newcol, missing)
#                 newcol[end] = val
#             end
#             ds[!, colname] = newcol
#         end
#         return ds
#     end
#
#     current_col = 0
#     try
#         if cols === :orderequal
#             if _names(ds) != _names(dsr)
#                 msg = "when `cols == :orderequal` pushed row must have the same " *
#                       "column names and in the same order as the target data frame"
#                 throw(ArgumentError(msg))
#             end
#         elseif cols === :setequal
#             msg = "Number of columns of `DatasetRow` does not match that of " *
#                   "target data frame (got $(length(dsr)) and $ncols)."
#             ncols == length(dsr) || throw(ArgumentError(msg))
#         end
#         for (col, nm) in zip(_columns(ds), _names(ds))
#             current_col += 1
#             if cols === :subset
#                 val = get(dsr, nm, missing)
#             else
#                 val = dsr[nm]
#             end
#             S = typeof(val)
#             T = eltype(col)
#             if S <: T || !promote || promote_type(S, T) <: T
#                 push!(col, val)
#             else
#                 newcol = similar(col, promote_type(S, T), targetrows)
#                 copyto!(newcol, 1, col, 1, nrows)
#                 newcol[end] = val
#                 firstindex(newcol) != 1 && _onebased_check_error()
#                 _columns(ds)[columnindex(ds, nm)] = newcol
#             end
#         end
#         for col in _columns(ds)
#             @assert length(col) == targetrows
#         end
#     catch err
#         for col in _columns(ds)
#             resize!(col, nrows)
#         end
#         if current_col > 0
#             @error "Error adding value to column :$(_names(ds)[current_col])."
#         end
#         rethrow(err)
#     end
#     return ds
# end
