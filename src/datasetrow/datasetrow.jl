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
julia> df = Dataset(a = repeat([1, 2], outer=[2]),
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

julia> df[1, :]
DatasetRow
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1

julia> @view df[end, [:a]]
DatasetRow
 Row │ a
     │ Int64
─────┼───────
   4 │     2

julia> eachrow(df)[1]
DatasetRow
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1

julia> Tuple(df[1, :])
(1, "a", 1)

julia> NamedTuple(df[1, :])
(a = 1, b = "a", c = 1)

julia> Vector(df[1, :])
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
    df::D
    colindex::S
    dfrow::Int # row number in df
    rownumber::Int # row number in the direct source AbstractDataset from which DatasetRow was created

    @inline DatasetRow(df::D, colindex::S, row::Union{Signed, Unsigned},
                         rownumber::Union{Signed, Unsigned}) where
        {D<:AbstractDataset, S<:AbstractIndex} = new{D, S}(df, colindex, row, rownumber)
end

Base.@propagate_inbounds function DatasetRow(df::Dataset, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(df, 1), row)
        throw(BoundsError(df, (row, cols)))
    end
    DatasetRow(df, SubIndex(index(df), cols), row, row)
end

Base.@propagate_inbounds DatasetRow(df::Dataset, row::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds function DatasetRow(sdf::SubDataset, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(sdf, 1), row)
        throw(BoundsError(sdf, (row, cols)))
    end
    if index(sdf) isa Index # sdf was created using : as row selector
        colindex = SubIndex(index(sdf), cols)
    else
        colindex = SubIndex(index(parent(sdf)), parentcols(index(sdf), cols))
    end
    @inbounds DatasetRow(parent(sdf), colindex, rows(sdf)[row], row)
end

Base.@propagate_inbounds DatasetRow(df::SubDataset, row::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds DatasetRow(df::AbstractDataset, row::Integer) =
    DatasetRow(df, row, :)

row(r::DatasetRow) = getfield(r, :dfrow)

"""
    rownumber(dfr::DatasetRow)

Return a row number in the `AbstractDataset` that `dfr` was created from.

Note that this differs from the first element in the tuple returned by
`parentindices`. The latter gives the row number in the `parent(dfr)`, which is
the source `Dataset` where data that `dfr` gives access to is stored.

# Examples
```jldoctest
julia> df = Dataset(reshape(1:12, 3, 4), :auto)
3×4 Dataset
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12

julia> dfr = df[2, :]
DatasetRow
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   2 │     2      5      8     11

julia> rownumber(dfr)
2

julia> parentindices(dfr)
(2, Base.OneTo(4))

julia> parent(dfr)
3×4 Dataset
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12

julia> dfv = @view df[2:3, 1:3]
2×3 SubDataset
 Row │ x1     x2     x3
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      5      8
   2 │     3      6      9

julia> dfrv = dfv[2, :]
DatasetRow
 Row │ x1     x2     x3
     │ Int64  Int64  Int64
─────┼─────────────────────
   3 │     3      6      9

julia> rownumber(dfrv)
2

julia> parentindices(dfrv)
(3, 1:3)

julia> parent(dfrv)
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

Base.parent(r::DatasetRow) = getfield(r, :df)
Base.parentindices(r::DatasetRow) = (row(r), parentcols(index(r)))

Base.summary(dfr::DatasetRow) = # -> String
    @sprintf("%d-element %s", length(dfr), nameof(typeof(dfr)))
Base.summary(io::IO, dfr::DatasetRow) = print(io, summary(dfr))

Base.@propagate_inbounds Base.view(adf::AbstractDataset, rowind::Integer,
                                   colinds::MultiColumnIndex) =
    DatasetRow(adf, rowind, colinds)

Base.@propagate_inbounds Base.getindex(df::AbstractDataset, rowind::Integer,
                                       colinds::MultiColumnIndex) =
    DatasetRow(df, rowind, colinds)
Base.@propagate_inbounds Base.getindex(df::AbstractDataset, rowind::Integer, ::Colon) =
    DatasetRow(df, rowind, :)
Base.@propagate_inbounds Base.getindex(r::DatasetRow, idx::ColumnIndex) =
    parent(r)[row(r), parentcols(index(r), idx)]

Base.@propagate_inbounds function Base.getindex(r::DatasetRow, idxs::MultiColumnIndex)
    # we create a temporary DatasetRow object to compute the SubIndex
    # in the parent(r), but this object has an incorrect rownumber
    # so we later copy rownumber from r
    # the Julia compiler should be able to optimize out this indirection
    # and in this way we avoid duplicating the code that computes the correct SubIndex
    dfr_tmp = DatasetRow(parent(r), row(r), parentcols(index(r), idxs))
    return DatasetRow(parent(dfr_tmp), index(dfr_tmp), row(r), rownumber(r))
end

Base.@propagate_inbounds Base.getindex(r::DatasetRow, ::Colon) = r

for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(df::Dataset,
                                  v::Union{DatasetRow, NamedTuple, AbstractDict},
                                  row_ind::Integer,
                                  col_inds::$(T))
        idxs = index(df)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned " *
                                    "collection contains $(length(v)) elements"))
        end

        if v isa AbstractDict
            if keytype(v) !== Symbol &&
                (keytype(v) <: AbstractString || all(x -> x isa AbstractString, keys(v)))
                v = (;(Symbol.(keys(v)) .=> values(v))...)
            end
            for n in view(_names(df), idxs)
                if !haskey(v, n)
                    throw(ArgumentError("Column :$n not found in source dictionary"))
                end
            end
        elseif !all(((a, b),) -> a == b, zip(view(_names(df), idxs), keys(v)))
            mismatched = findall(view(_names(df), idxs) .!= collect(keys(v)))
            throw(ArgumentError("Selected column names do not match the names in assigned " *
                                "value in positions $(join(mismatched, ", ", " and "))"))
        end

        for (col, val) in pairs(v)
            df[row_ind, col] = val
        end
        return df
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
    dfr_tmp = DatasetRow(parent(r), row(r), parentcols(index(r), cols))
    return DatasetRow(parent(dfr_tmp), index(dfr_tmp), row(r), rownumber(r))
end

Base.view(r::DatasetRow, ::Colon) = r

"""
    size(dfr::DatasetRow[, dim])

Return a 1-tuple containing the number of elements of `dfr`.
If an optional dimension `dim` is specified, it must be `1`, and the number of
elements is returned directly as a number.

See also: [`length`](@ref)

# Examples
```jldoctest
julia> dfr = Dataset(a=1:3, b='a':'c')[1, :]
DatasetRow
 Row │ a      b
     │ Int64  Char
─────┼─────────────
   1 │     1  a

julia> size(dfr)
(2,)

julia> size(dfr, 1)
2
```
"""
Base.size(r::DatasetRow) = (length(index(r)),)
Base.size(r::DatasetRow, i) = size(r)[i]

"""
    length(dfr::DatasetRow)

Return the number of elements of `dfr`.

See also: [`size`](@ref)

# Examples
```jldoctest
julia> dfr = Dataset(a=1:3, b='a':'c')[1, :]
DatasetRow
 Row │ a      b
     │ Int64  Char
─────┼─────────────
   1 │     1  a

julia> length(dfr)
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

function Base.Vector(dfr::DatasetRow)
    df = parent(dfr)
    T = reduce(promote_type, (eltype(df[!, i]) for i in parentcols(index(dfr))))
    return Vector{T}(dfr)
end
Base.Vector{T}(dfr::DatasetRow) where T =
    T[dfr[i] for i in 1:length(dfr)]

Base.Array(dfr::DatasetRow) = Vector(dfr)
Base.Array{T}(dfr::DatasetRow) where {T} = Vector{T}(dfr)

Base.keys(r::DatasetRow) = propertynames(r)
Base.values(r::DatasetRow) =
    ntuple(col -> parent(r)[row(r), parentcols(index(r), col)], length(r))
Base.map(f, r::DatasetRow, rs::DatasetRow...) = map(f, copy(r), copy.(rs)...)
Base.get(dfr::DatasetRow, key::ColumnIndex, default) =
    haskey(dfr, key) ? dfr[key] : default
Base.get(f::Base.Callable, dfr::DatasetRow, key::ColumnIndex) =
    haskey(dfr, key) ? dfr[key] : f()
Base.broadcastable(::DatasetRow) =
    throw(ArgumentError("broadcasting over `DatasetRow`s is reserved"))

function Base.NamedTuple(dfr::DatasetRow)
    k = Tuple(_names(dfr))
    v = ntuple(i -> dfr[i], length(dfr))
    pc = parentcols(index(dfr))
    cols = _columns(parent(dfr))
    s = ntuple(i -> eltype(cols[pc[i]]), length(dfr))
    NamedTuple{k, Tuple{s...}}(v)
end

"""
    copy(dfr::DatasetRow)

Construct a `NamedTuple` with the same contents as the [`DatasetRow`](@ref).
This method returns a `NamedTuple` so that the returned object
is not affected by changes to the parent data frame of which `dfr` is a view.

"""
Base.copy(dfr::DatasetRow) = NamedTuple(dfr)

Base.convert(::Type{NamedTuple}, dfr::DatasetRow) = NamedTuple(dfr)

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

function Dataset(dfr::DatasetRow)
    row, cols = parentindices(dfr)
    parent(dfr)[row:row, cols]
end

@noinline pushhelper!(x, r) = push!(x, x[r])

function Base.push!(df::Dataset, dfr::DatasetRow; cols::Symbol=:setequal,
                    promote::Bool=(cols in [:union, :subset]))
    possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
    if !(cols in possible_cols)
        throw(ArgumentError("`cols` keyword argument must be any of :" *
                            join(possible_cols, ", :")))
    end

    nrows, ncols = size(df)
    targetrows = nrows + 1

    if parent(dfr) === df && index(dfr) isa Index
        # in this case we are sure that all we do is safe
        r = row(dfr)
        for col in _columns(df)
            # use a barrier function to improve performance
            pushhelper!(col, r)
        end
        for (colname, col) in zip(_names(df), _columns(df))
            if length(col) != targetrows
                for col2 in _columns(df)
                    resize!(col2, nrows)
                end
                throw(AssertionError("Error adding value to column :$colname"))
            end
        end
        return df
    end

    if ncols == 0
        for (n, v) in pairs(dfr)
            setproperty!(df, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
        end
        return df
    end

    if cols == :union
        for (i, colname) in enumerate(_names(df))
            col = _columns(df)[i]
            if hasproperty(dfr, colname)
                val = dfr[colname]
            else
                val = missing
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || promote_type(S, T) <: T
                push!(col, val)
            elseif !promote
                try
                    push!(col, val)
                catch err
                    for col in _columns(df)
                        resize!(col, nrows)
                    end
                    @error "Error adding value to column :$colname."
                    rethrow(err)
                end
            else
                newcol = Tables.allocatecolumn(promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(df)[i] = newcol
            end
        end
        for (colname, col) in zip(_names(df), _columns(df))
            if length(col) != targetrows
                for col2 in _columns(df)
                    resize!(col2, nrows)
                end
                throw(AssertionError("Error adding value to column :$colname"))
            end
        end
        for colname in setdiff(_names(dfr), _names(df))
            val = dfr[colname]
            S = typeof(val)
            if nrows == 0
                newcol = [val]
            else
                newcol = Tables.allocatecolumn(Union{Missing, S}, targetrows)
                fill!(newcol, missing)
                newcol[end] = val
            end
            df[!, colname] = newcol
        end
        return df
    end

    current_col = 0
    try
        if cols === :orderequal
            if _names(df) != _names(dfr)
                msg = "when `cols == :orderequal` pushed row must have the same " *
                      "column names and in the same order as the target data frame"
                throw(ArgumentError(msg))
            end
        elseif cols === :setequal
            msg = "Number of columns of `DatasetRow` does not match that of " *
                  "target data frame (got $(length(dfr)) and $ncols)."
            ncols == length(dfr) || throw(ArgumentError(msg))
        end
        for (col, nm) in zip(_columns(df), _names(df))
            current_col += 1
            if cols === :subset
                val = get(dfr, nm, missing)
            else
                val = dfr[nm]
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                push!(col, val)
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(df)[columnindex(df, nm)] = newcol
            end
        end
        for col in _columns(df)
            @assert length(col) == targetrows
        end
    catch err
        for col in _columns(df)
            resize!(col, nrows)
        end
        if current_col > 0
            @error "Error adding value to column :$(_names(df)[current_col])."
        end
        rethrow(err)
    end
    return df
end
