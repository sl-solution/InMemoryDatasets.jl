"""
    Dataset <: AbstractDataset

An AbstractDataset that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector or CategoricalVector.

# Constructors
```julia
Dataset(pairs::Pair...; makeunique::Bool=false, copycols::Bool=true)
Dataset(pairs::AbstractVector{<:Pair}; makeunique::Bool=false, copycols::Bool=true)
Dataset(ds::AbstractDict; copycols::Bool=true)
Dataset(kwargs..., copycols::Bool=true)

Dataset(columns::AbstractVecOrMat, names::Union{AbstractVector, Symbol};
          makeunique::Bool=false, copycols::Bool=true)

Dataset(table; copycols::Bool=true)
Dataset(::DatasetRow)
Dataset(::GroupedDataset; keepkeys::Bool=true)
```

# Keyword arguments

- `copycols` : whether vectors passed as columns should be copied; by default set
  to `true` and the vectors are copied; if set to `false` then the constructor
  will still copy the passed columns if it is not possible to construct a
  `Dataset` without materializing new columns.
- `makeunique` : if `false` (the default), an error will be raised

(note that not all constructors support these keyword arguments)

# Details on behavior of different constructors

It is allowed to pass a vector of `Pair`s, a list of `Pair`s as positional
arguments, or a list of keyword arguments. In this case each pair is considered
to represent a column name to column value mapping and column name must be a
`Symbol` or string. Alternatively a dictionary can be passed to the constructor
in which case its entries are considered to define the column name and column
value pairs. If the dictionary is a `Dict` then column names will be sorted in
the returned `Dataset`.

In all the constructors described above column value can be a vector which is
consumed as is or an object of any other type (except `AbstractArray`). In the
latter case the passed value is automatically repeated to fill a new vector of
the appropriate length. As a particular rule values stored in a `Ref` or a
`0`-dimensional `AbstractArray` are unwrapped and treated in the same way.

It is also allowed to pass a vector of vectors or a matrix as as the first
argument. In this case the second argument must be
a vector of `Symbol`s or strings specifying column names, or the symbol `:auto`
to generate column names `x1`, `x2`, ... automatically.

If a single positional argument is passed to a `Dataset` constructor then it
is assumed to be of type that implements the
[Tables.jl](https://github.com/JuliaData/Tables.jl) interface using which the
returned `Dataset` is materialized.

Finally it is allowed to construct a `Dataset` from a `DatasetRow` or a
`GroupedDataset`. In the latter case the `keepkeys` keyword argument specifies
whether the resulting `Dataset` should contain the grouping columns of the
passed `GroupedDataset` and the order of rows in the result follows the order
of groups in the `GroupedDataset` passed.

# Notes

The `Dataset` constructor by default copies all columns vectors passed to it.
Pass the `copycols=false` keyword argument (where supported) to reuse vectors without
copying them.

By default an error will be raised if duplicates in column names are found. Pass
`makeunique=true` keyword argument (where supported) to accept duplicate names,
in which case they will be suffixed with `_i` (`i` starting at 1 for the first
duplicate).

If an `AbstractRange` is passed to a `Dataset` constructor as a column it is
always collected to a `Vector` (even if `copycols=false`). As a general rule
`AbstractRange` values are always materialized to a `Vector` by all functions in
InMemoryDatasets.jl before being stored in a `Dataset`.

`Dataset` can store only columns that use 1-based indexing. Attempting
to store a vector using non-standard indexing raises an error.

The `Dataset` type is designed to allow column types to vary and to be
dynamically changed also after it is constructed. Therefore `Dataset`s are not
type stable. For performance-critical code that requires type-stability either
use the functionality provided by `select`/`transform`/`combine` functions, use
`Tables.columntable` and `Tables.namedtupleiterator` functions, use barrier
functions, or provide type assertions to the variables that hold columns
extracted from a `Dataset`.

# Examples
```jldoctest
julia> Dataset((a=[1, 2], b=[3, 4])) # Tables.jl table constructor
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> Dataset([(a=1, b=0), (a=2, b=0)]) # Tables.jl table constructor
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> Dataset("a" => 1:2, "b" => 0) # Pair constructor
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> Dataset([:a => 1:2, :b => 0]) # vector of Pairs constructor
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> Dataset(Dict(:a => 1:2, :b => 0)) # dictionary constructor
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> Dataset(a=1:2, b=0) # keyword argument constructor
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> Dataset([[1, 2], [0, 0]], [:a, :b]) # vector of vectors constructor
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> Dataset([1 0; 2 0], :auto) # matrix constructor
2×2 Dataset
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0
```
"""
struct Dataset <: AbstractDataset
    columns::Vector{AbstractVector}
    colindex::Index
    attributes::Attributes
    # the inner constructor should not be used directly
    function Dataset(columns::Union{Vector{Any}, Vector{AbstractVector}},
                       colindex::Index; copycols::Bool=true)
        if length(columns) == length(colindex) == 0
            return new(AbstractVector[], Index(), Attributes())
        elseif length(columns) != length(colindex)
            throw(DimensionMismatch("Number of columns ($(length(columns))) and number of " *
                                    "column names ($(length(colindex))) are not equal"))
        end

        len = -1
        firstvec = -1
        for (i, col) in enumerate(columns)
            if col isa AbstractVector
                if len == -1
                    len = length(col)
                    firstvec = i
                elseif len != length(col)
                    n1 = _names(colindex)[firstvec]
                    n2 = _names(colindex)[i]
                    throw(DimensionMismatch("column :$n1 has length $len and column " *
                                            ":$n2 has length $(length(col))"))
                end
            end
        end
        len == -1 && (len = 1) # we got no vectors so make one row of scalars
        # it is not good idea to use threads when we have many rows (memory wise)
        if length(columns) > 100
            Threads.@threads for i in eachindex(columns)
              columns[i] = _preprocess_column(columns[i], len, copycols)
            end
        else
            for i in eachindex(columns)
              columns[i] = _preprocess_column(columns[i], len, copycols)
            end
        end

        for (i, col) in enumerate(columns)
            firstindex(col) != 1 && _onebased_check_error(i, col)
        end

        new(convert(Vector{AbstractVector}, columns), colindex, Attributes())
    end
end

function _preprocess_column(col::Any, len::Integer, copycols::Bool)
    if col isa AbstractRange
        return collect(col)
    elseif col isa AbstractVector
        return copycols ? copy(col) : col
    elseif col isa Union{AbstractArray{<:Any, 0}, Ref}
        x = col[]
        return fill!(Tables.allocatecolumn(typeof(x), len), x)
    elseif col isa AbstractArray
        throw(ArgumentError("adding AbstractArray other than AbstractVector " *
                            "as a column of a data set is not allowed"))
    else
        return fill!(Tables.allocatecolumn(typeof(col), len), col)
    end
end

# Create Dataset
Dataset(df::Dataset; copycols::Bool=true) = copy(df, copycols=copycols)

# Create Dataset
function Dataset(pairs::Pair{Symbol, <:Any}...; makeunique::Bool=false,
                   copycols::Bool=true)::Dataset
    colnames = [Symbol(k) for (k, v) in pairs]
    columns = Any[v for (k, v) in pairs]
    return Dataset(columns, Index(colnames, makeunique=makeunique),
                     copycols=copycols)
end

# Create Dataset
function Dataset(pairs::Pair{<:AbstractString, <:Any}...; makeunique::Bool=false,
                   copycols::Bool=true)::Dataset
    colnames = [Symbol(k) for (k, v) in pairs]
    columns = Any[v for (k, v) in pairs]
    return Dataset(columns, Index(colnames, makeunique=makeunique),
                     copycols=copycols)
end


# Create Dataset
# this is needed as a workaround for Tables.jl dispatch
function Dataset(pairs::AbstractVector{<:Pair}; makeunique::Bool=false,
                   copycols::Bool=true)
    if isempty(pairs)
        return Dataset()
    else
        if !(all(((k, v),) -> k isa Symbol, pairs) || all(((k, v),) -> k isa AbstractString, pairs))
            throw(ArgumentError("All column names must be either Symbols or strings (mixing is not allowed)"))
        end
        colnames = [Symbol(k) for (k, v) in pairs]
        columns = Any[v for (k, v) in pairs]
        return Dataset(columns, Index(colnames, makeunique=makeunique),
                         copycols=copycols)
    end
end

# Create Dataset
function Dataset(d::AbstractDict; copycols::Bool=true)
    if all(k -> k isa Symbol, keys(d))
        colnames = collect(Symbol, keys(d))
    elseif all(k -> k isa AbstractString, keys(d))
        colnames = [Symbol(k) for k in keys(d)]
    else
        throw(ArgumentError("All column names must be either Symbols or strings (mixing is not allowed)"))
    end

    colindex = Index(colnames)
    columns = Any[v for v in values(d)]
    df = Dataset(columns, colindex, copycols=copycols)
    d isa Dict && select!(df, sort!(propertynames(df)))
    return df
end

# Create Dataset
function Dataset(; kwargs...)
    if isempty(kwargs)
        Dataset([], Index())
    else
        cnames = Symbol[]
        columns = Any[]
        copycols = true
        for (kw, val) in kwargs
            if kw === :copycols
                if val isa Bool
                    copycols = val
                else
                    throw(ArgumentError("the `copycols` keyword argument must be Boolean"))
                end
            elseif kw === :makeunique
                    throw(ArgumentError("the `makeunique` keyword argument is not allowed " *
                                        "in Dataset(; kwargs...) constructor"))
            else
                push!(cnames, kw)
                push!(columns, val)
            end
        end
        Dataset(columns, Index(cnames), copycols=copycols)
    end
end

# Create Dataset
function Dataset(columns::AbstractVector, cnames::AbstractVector{Symbol};
                   makeunique::Bool=false, copycols::Bool=true)::Dataset
    if !(eltype(columns) <: AbstractVector) && !all(col -> isa(col, AbstractVector), columns)
        throw(ArgumentError("columns argument must be a vector of AbstractVector objects"))
    end
    return Dataset(collect(AbstractVector, columns),
                     Index(convert(Vector{Symbol}, cnames), makeunique=makeunique),
                     copycols=copycols)
end

# Create Dataset
Dataset(columns::AbstractVector, cnames::AbstractVector{<:AbstractString};
          makeunique::Bool=false, copycols::Bool=true) =
    Dataset(columns, Symbol.(cnames), makeunique=makeunique, copycols=copycols)

# Create Dataset
Dataset(columns::AbstractVector{<:AbstractVector}, cnames::AbstractVector{Symbol};
          makeunique::Bool=false, copycols::Bool=true)::Dataset =
    Dataset(collect(AbstractVector, columns),
              Index(convert(Vector{Symbol}, cnames), makeunique=makeunique),
              copycols=copycols)

# Create Dataset
Dataset(columns::AbstractVector{<:AbstractVector}, cnames::AbstractVector{<:AbstractString};
          makeunique::Bool=false, copycols::Bool=true) =
    Dataset(columns, Symbol.(cnames); makeunique=makeunique, copycols=copycols)

# Create Dataset
function Dataset(columns::AbstractVector, cnames::Symbol; copycols::Bool=true)
    if cnames !== :auto
        throw(ArgumentError("if the first positional argument to Dataset " *
                            "constructor is a vector of vectors and the second " *
                            "positional argument is passed then the second " *
                            "argument must be a vector of column names or :auto"))
    end
    return Dataset(columns, gennames(length(columns)), copycols=copycols)
end

# Create Dataset
Dataset(columns::AbstractMatrix, cnames::AbstractVector{Symbol}; makeunique::Bool=false) =
    Dataset(AbstractVector[columns[:, i] for i in 1:size(columns, 2)], cnames,
              makeunique=makeunique, copycols=false)

# Create Dataset
Dataset(columns::AbstractMatrix, cnames::AbstractVector{<:AbstractString};
          makeunique::Bool=false) =
    Dataset(columns, Symbol.(cnames); makeunique=makeunique)

# Create Dataset
function Dataset(columns::AbstractMatrix, cnames::Symbol)
    if cnames !== :auto
        throw(ArgumentError("if the first positional argument to Dataset " *
                            "constructor is a matrix and a second " *
                            "positional argument is passed then the second " *
                            "argument must be a vector of column names or :auto"))
    end
    return Dataset(columns, gennames(size(columns, 2)), makeunique=false)
end

# Discontinued constructors

# Create Dataset
Dataset(matrix::Matrix) =
    throw(ArgumentError("`Dataset` constructor from a `Matrix` requires " *
                        "passing :auto as a second argument to automatically " *
                        "generate column names: `Dataset(matrix, :auto)`"))

# Create Dataset
Dataset(vecs::Vector{<:AbstractVector}) =
    throw(ArgumentError("`Dataset` constructor from a `Vector` of vectors requires " *
                        "passing :auto as a second argument to automatically " *
                        "generate column names: `Dataset(vecs, :auto)`"))

# Create Dataset
Dataset(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
          nrows::Integer=0; makeunique::Bool=false) where T<:Type =
    throw(ArgumentError("`Dataset` constructor with passed eltypes is " *
                        "not supported. Pass explicitly created columns to a " *
                        "`Dataset` constructor instead."))

# Create Dataset
Dataset(column_eltypes::AbstractVector{<:Type}, cnames::AbstractVector{<:AbstractString},
          nrows::Integer=0; makeunique::Bool=false) where T<:Type =
    throw(ArgumentError("`Dataset` constructor with passed eltypes is " *
                        "not supported. Pass explicitly created columns to a " *
                        "`Dataset` constructor instead."))


##############################################################################
##
## AbstractDataset interface
##
##############################################################################

index(ds::Dataset) = getfield(ds, :colindex)
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
_columns(ds::Dataset) = getfield(ds, :columns)

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

_check_consistency(ds::AbstractDataset) = _check_consistency(parent(ds))

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

# ds[MultiRowIndex, SingleColumnIndex] => AbstractVector, copy
@inline function Base.getindex(ds::Dataset, row_inds::AbstractVector, col_ind::ColumnIndex)
    selected_column = index(ds)[col_ind]
    @boundscheck if !checkindex(Bool, axes(ds, 1), row_inds)
        throw(BoundsError(ds, (row_inds, col_ind)))
    end
    @inbounds return _columns(ds)[selected_column][row_inds]
end

@inline Base.getindex(ds::Dataset, row_inds::Not, col_ind::ColumnIndex) =
    ds[axes(ds, 1)[row_inds], col_ind]

# ds[:, SingleColumnIndex] => AbstractVector
function Base.getindex(ds::Dataset, row_inds::Colon, col_ind::ColumnIndex)
    selected_column = index(ds)[col_ind]
    copy(_columns(ds)[selected_column])
end

# ds[!, SingleColumnIndex] => AbstractVector, the same vector
@inline function Base.getindex(ds::Dataset, ::typeof(!), col_ind::Union{Signed, Unsigned})
    cols = _columns(ds)
    @boundscheck if !checkindex(Bool, axes(cols, 1), col_ind)
        throw(BoundsError(ds, (!, col_ind)))
    end
    @inbounds cols[col_ind]
end

function Base.getindex(ds::Dataset, ::typeof(!), col_ind::SymbolOrString)
    selected_column = index(ds)[col_ind]
    return _columns(ds)[selected_column]
end

# ds[MultiRowIndex, MultiColumnIndex] => Dataset
function _threaded_permute(x, perm)
  x_cpy = similar(x, length(perm))
  Threads.@threads for i in 1:length(x_cpy)
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
        new_columns[j] = _threaded_permute(ds_columns[selected_columns[j]], selected_rows)
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
    dsformat = getfield(ds, :colindex).format
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
    idx = copy(index(ds))

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
Base.getindex(ds::Dataset, row_ind::Colon, col_inds::MultiColumnIndex) =
    select(ds, col_inds, copycols=true)

# df[!, MultiColumnIndex] => Dataset
# Create Dataset
Base.getindex(df::Dataset, row_ind::typeof(!), col_inds::MultiColumnIndex) =
    select(df, col_inds, copycols=false)

##############################################################################
##
## setindex!()
##
##############################################################################

# Will automatically add a new column if needed

# Modify Dataset
function insert_single_column!(ds::Dataset, v::AbstractVector, col_ind::ColumnIndex)
    if ncol(ds) != 0 && nrow(ds) != length(v)
        throw(ArgumentError("New columns must have the same length as old columns"))
    end
    dv = isa(v, AbstractRange) ? collect(v) : v
    firstindex(dv) != 1 && _onebased_check_error()

    if haskey(index(ds), col_ind)
        j = index(ds)[col_ind]
        _columns(ds)[j] = dv
        removeformat!(ds, j)
        _modified(_attributes(ds))
    else
        if col_ind isa SymbolOrString
            push!(index(ds), Symbol(col_ind))
            push!(_columns(ds), dv)
            _modified(_attributes(ds))
        else
            throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
        end
    end
    return dv
end

# Modify Dataset
function insert_single_entry!(ds::Dataset, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    if haskey(index(ds), col_ind)
      # single entry doesn't remove format
        _columns(ds)[index(ds)[col_ind]][row_ind] = v
        _modified(_attributes)
        return v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
    end
end

# ds[!, SingleColumnIndex] = AbstractVector

# Modify Dataset
function Base.setindex!(ds::Dataset, v::AbstractVector, ::typeof(!), col_ind::ColumnIndex)
    insert_single_column!(ds, v, col_ind)
    return ds
end

# ds.col = AbstractVector
# separate methods are needed due to dispatch ambiguity

# Modify Dataset
function Base.setproperty!(ds::Dataset, col_ind::Symbol, v::AbstractVector)
    ds[!, col_ind] = v
    v
end

# Modify Dataset
function Base.setproperty!(ds::Dataset, col_ind::AbstractString, v::AbstractVector)
    ds[!, col_ind] = v
    v
end

# Modify Dataset
Base.setproperty!(::Dataset, col_ind::Symbol, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a Dataset. " *
                        "Instead use `ds[!, col_ind] .= v` if you want to use broadcasting."))

# Modify Dataset
Base.setproperty!(::Dataset, col_ind::AbstractString, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a Dataset. " *
                        "Instead use `ds[!, col_ind] .= v` if you want to use broadcasting."))

# ds[SingleRowIndex, SingleColumnIndex] = Single Item

# Modify Dataset
function Base.setindex!(ds::Dataset, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    insert_single_entry!(ds, v, row_ind, col_ind)
    return ds
end

# ds[SingleRowIndex, MultiColumnIndex] = value
# the method for value of type DatasetRow, AbstractDict and NamedTuple
# is defined in datasetrow.jl

# Modify Dataset
for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  v::Union{Tuple, AbstractArray},
                                  row_ind::Integer,
                                  col_inds::$T)
        idxs = index(ds)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned " *
                                    "collection contains $(length(v)) elements"))
        end
        for (i, x) in zip(idxs, v)
            ds[row_ind, i] = x
        end
        return ds
    end
end

# ds[MultiRowIndex, SingleColumnIndex] = AbstractVector

# Modify Dataset
for T in (:AbstractVector, :Not, :Colon)
    @eval function Base.setindex!(ds::Dataset,
                                  v::AbstractVector,
                                  row_inds::$T,
                                  col_ind::ColumnIndex)
        if row_inds isa Colon && !haskey(index(ds), col_ind)
            ds[!, col_ind] = copy(v)
            return ds
        end
        x = ds[!, col_ind]
        x[row_inds] = v
        return ds
    end
end

# ds[MultiRowIndex, MultiColumnIndex] = AbstractDataset

# Modify Dataset
for T1 in (:AbstractVector, :Not, :Colon),
    T2 in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  new_ds::AbstractDataset,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(ds)[col_inds]
        if view(_names(ds), idxs) != _names(new_ds)
            throw(ArgumentError("column names in source and target do not match"))
        end
        for (j, col) in enumerate(idxs)
            ds[row_inds, col] = new_ds[!, j]
        end
        return ds
    end
end

# Modify Dataset
for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  new_ds::AbstractDataset,
                                  row_inds::typeof(!),
                                  col_inds::$T)
        idxs = index(ds)[col_inds]
        if view(_names(ds), idxs) != _names(new_ds)
            throw(ArgumentError("Column names in source and target data sets do not match"))
        end
        for (j, col) in enumerate(idxs)
            # make sure we make a copy on assignment
            ds[!, col] = new_ds[:, j]
        end
        return ds
    end
end

# ds[MultiRowIndex, MultiColumnIndex] = AbstractMatrix

# Modify Dataset
for T1 in (:AbstractVector, :Not, :Colon, :(typeof(!))),
    T2 in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  mx::AbstractMatrix,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(ds)[col_inds]
        if size(mx, 2) != length(idxs)
            throw(DimensionMismatch("number of selected columns ($(length(idxs))) " *
                                    "and number of columns in " *
                                    "matrix ($(size(mx, 2))) do not match"))
        end
        for (j, col) in enumerate(idxs)
            ds[row_inds, col] = (row_inds === !) ? mx[:, j] : view(mx, :, j)
        end
        return ds
    end
end


##############################################################################
##
## Mutating methods
##
##############################################################################

"""
    insertcols!(ds::Dataset[, col], (name=>val)::Pair...;
                makeunique::Bool=false, copycols::Bool=true)

Insert a column into a data set in place. Return the updated `Dataset`.
If `col` is omitted it is set to `ncol(ds)+1`
(the column is inserted as the last column).

# Arguments
- `ds` : the Dataset to which we want to add columns
- `col` : a position at which we want to insert a column, passed as an integer
  or a column name (a string or a `Symbol`); the column selected with `col`
  and columns following it are shifted to the right in `ds` after the operation
- `name` : the name of the new column
- `val` : an `AbstractVector` giving the contents of the new column or a value of any
  type other than `AbstractArray` which will be repeated to fill a new vector;
  As a particular rule a values stored in a `Ref` or a `0`-dimensional `AbstractArray`
  are unwrapped and treated in the same way.
- `makeunique` : Defines what to do if `name` already exists in `ds`;
  if it is `false` an error will be thrown; if it is `true` a new unique name will
  be generated by adding a suffix
- `copycols` : whether vectors passed as columns should be copied

If `val` is an `AbstractRange` then the result of `collect(val)` is inserted.

# Examples
```jldoctest
julia> ds = Dataset(a=1:3)
3×1 Dataset
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> insertcols!(ds, 1, :b => 'a':'c')
3×2 Dataset
 Row │ b     a
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> insertcols!(ds, 2, :c => 2:4, :c => 3:5, makeunique=true)
3×4 Dataset
 Row │ b     c      c_1    a
     │ Char  Int64  Int64  Int64
─────┼───────────────────────────
   1 │ a         2      3      1
   2 │ b         3      4      2
   3 │ c         4      5      3
```
"""
function insertcols!(ds::Dataset, col::ColumnIndex, name_cols::Pair{Symbol, <:Any}...;
                     makeunique::Bool=false, copycols::Bool=true)

# Modify Dataset
    col_ind = Int(col isa SymbolOrString ? columnindex(ds, col) : col)
    if !(0 < col_ind <= ncol(ds) + 1)
        throw(ArgumentError("attempt to insert a column to a data set with " *
                            "$(ncol(ds)) columns at index $col_ind"))
    end

    if !makeunique
        if !allunique(first.(name_cols))
            throw(ArgumentError("Names of columns to be inserted into a data set " *
                                "must be unique when `makeunique=true`"))
        end
        for (n, _) in name_cols
            if hasproperty(ds, n)
                throw(ArgumentError("Column $n is already present in the data set " *
                                    "which is not allowed when `makeunique=true`"))
            end
        end
    end

    if ncol(ds) == 0
        target_row_count = -1
    else
        target_row_count = nrow(ds)
    end

    for (n, v) in name_cols
        if v isa AbstractVector
            if target_row_count == -1
                target_row_count = length(v)
            elseif length(v) != target_row_count
                if target_row_count == nrow(ds)
                    throw(DimensionMismatch("length of new column $n which is " *
                                            "$(length(v)) must match the number " *
                                            "of rows in data set ($(nrow(ds)))"))
                else
                    throw(DimensionMismatch("all vectors passed to be inserted into " *
                                            "a data set must have the same length"))
                end
            end
        elseif v isa AbstractArray && ndims(v) > 1
            throw(ArgumentError("adding AbstractArray other than AbstractVector as " *
                                "a column of a data set is not allowed"))
        end
    end
    if target_row_count == -1
        target_row_count = 1
    end

    for (name, item) in name_cols
        if !(item isa AbstractVector)
            if item isa Union{AbstractArray{<:Any, 0}, Ref}
                x = item[]
                item_new = fill!(Tables.allocatecolumn(typeof(x), target_row_count), x)
            else
                @assert !(item isa AbstractArray)
                item_new = fill!(Tables.allocatecolumn(typeof(item), target_row_count), item)
            end
        elseif item isa AbstractRange
            item_new = collect(item)
        elseif copycols
            item_new = copy(item)
        else
            item_new = item
        end

        firstindex(item_new) != 1 && _onebased_check_error()

        if ncol(ds) == 0
            ds[!, name] = item_new
        else
            if hasproperty(ds, name)
                @assert makeunique
                k = 1
                while true
                    nn = Symbol("$(name)_$k")
                    if !hasproperty(ds, nn)
                        name = nn
                        break
                    end
                    k += 1
                end
            end
            insert!(index(ds), col_ind, name)
            insert!(_columns(ds), col_ind, item_new)
            _modified(_attributes(ds))
        end
        col_ind += 1
    end
    return ds
end

# Modify Dataset
insertcols!(ds::Dataset, col::ColumnIndex, name_cols::Pair{<:AbstractString, <:Any}...;
                     makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(ds, col, (Symbol(n) => v for (n, v) in name_cols)...,
                makeunique=makeunique, copycols=copycols)

# Modify Dataset
insertcols!(ds::Dataset, name_cols::Pair{Symbol, <:Any}...;
            makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(ds, ncol(ds)+1, name_cols..., makeunique=makeunique, copycols=copycols)

# Modify Dataset
insertcols!(ds::Dataset, name_cols::Pair{<:AbstractString, <:Any}...;
            makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(ds, (Symbol(n) => v for (n, v) in name_cols)...,
                makeunique=makeunique, copycols=copycols)

# Modify Dataset
function insertcols!(ds::Dataset, col::Int=ncol(ds)+1; makeunique::Bool=false, name_cols...)
    if !(0 < col <= ncol(ds) + 1)
        throw(ArgumentError("attempt to insert a column to a data set with " *
                            "$(ncol(ds)) columns at index $col"))
    end
    if !isempty(name_cols)
        # an explicit error is thrown as keyword argument was supported in the past
        throw(ArgumentError("inserting colums using a keyword argument is not supported, " *
                            "pass a Pair as a positional argument instead"))
    end
    return ds
end

"""
    copy(ds::Dataset; copycols::Bool=true)

Copy data set `ds`.
If `copycols=true` (the default), return a new  `Dataset` holding
copies of column vectors in `ds`.
If `copycols=false`, return a new `Dataset` sharing column vectors with `ds`.
"""
function Base.copy(ds::Dataset; copycols::Bool=true)

# Create Dataset
    newds = Dataset(copy(_columns(ds)), copy(index(ds)), copycols=copycols)
    setinfo!(newds, _attributes(ds).meta.info[])
    return newds
end

"""
    delete!(ds::Dataset, inds)

Delete rows specified by `inds` from a `Dataset` `ds` in place and return it.

Internally `deleteat!` is called for all columns so `inds` must be:
a vector of sorted and unique integers, a boolean vector, an integer, or `Not`.

# Examples
```jldoctest
julia> ds = Dataset(a=1:3, b=4:6)
3×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6

julia> delete!(ds, 2)
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     3      6
```

"""
function Base.delete!(ds::Dataset, inds)

# Modify Dataset
    if !isempty(inds) && size(ds, 2) == 0
        throw(BoundsError(ds, (inds, :)))
    end

    # we require ind to be stored and unique like in Base
    # otherwise an error will be thrown and the data set will get corrupted
    return _delete!_helper(ds, inds)
end

# Modify Dataset
function Base.delete!(ds::Dataset, inds::AbstractVector{Bool})
    if length(inds) != size(ds, 1)
        throw(BoundsError(ds, (inds, :)))
    end
    drop = _findall(inds)
    return _delete!_helper(ds, drop)
end

# Modify Dataset
Base.delete!(ds::Dataset, inds::Not) = delete!(ds, axes(ds, 1)[inds])

# Modify Dataset
function _delete!_helper(ds::Dataset, drop)
    cols = _columns(ds)
    isempty(cols) && return ds

    n = nrow(ds)
    col1 = cols[1]
    deleteat!(col1, drop)
    newn = length(col1)

    for i in 2:length(cols)
        col = cols[i]
        if length(col) == n
            deleteat!(col, drop)
        end
    end

    for i in 1:length(cols)
        # this should never happen, but we add it for safety
        @assert length(cols[i]) == newn corrupt_msg(ds, i)
    end
    _modified(_attributes(ds))
    return ds
end

"""
    empty!(ds::Dataset)

Remove all rows from `ds`, making each of its columns empty.
"""
function Base.empty!(ds::Dataset)

# Modify Dataset
    foreach(empty!, eachcol(ds))
    _modified(_attributes(ds))
    return ds
end

##############################################################################
##
## Hcat specialization
##
##############################################################################

# hcat! for 2 arguments, only a vector or a data set is allowed

# Modify Dataset
function hcat!(ds1::Dataset, ds2::AbstractDataset;
               makeunique::Bool=false, copycols::Bool=true)
    u = add_names(index(ds1), index(ds2), makeunique=makeunique)
    for i in 1:length(u)
        ds1[!, u[i]] = copycols ? ds2[:, i] : ds2[!, i]
    end
    return ds1
end

# definition required to avoid hcat! ambiguity

# Modify Dataset
hcat!(ds1::Dataset, ds2::Dataset;
      makeunique::Bool=false, copycols::Bool=true) =
    invoke(hcat!, Tuple{Dataset, AbstractDataset}, ds1, ds2,
           makeunique=makeunique, copycols=copycols)::Dataset

# Modify Dataset
hcat!(ds::Dataset, x::AbstractVector; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(ds, Dataset(AbstractVector[x], [:x1], copycols=copycols),
          makeunique=makeunique, copycols=copycols)

# Modify Dataset
hcat!(x::AbstractVector, ds::Dataset; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(Dataset(AbstractVector[x], [:x1], copycols=copycols), ds,
          makeunique=makeunique, copycols=copycols)

# Modify Dataset
hcat!(x, ds::Dataset; makeunique::Bool=false, copycols::Bool=true) =
    throw(ArgumentError("x must be AbstractVector or AbstractDataset"))

# Modify Dataset
hcat!(ds::Dataset, x; makeunique::Bool=false, copycols::Bool=true) =
    throw(ArgumentError("x must be AbstractVector or AbstractDataset"))

# hcat! for 1-n arguments

# Modify Dataset
hcat!(ds::Dataset; makeunique::Bool=false, copycols::Bool=true) = df

# Modify Dataset
hcat!(a::Dataset, b, c...; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat!(a, b, makeunique=makeunique, copycols=copycols),
          c..., makeunique=makeunique, copycols=copycols)

# hcat

# Create Dataset
Base.hcat(ds::Dataset, x; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(copy(ds, copycols=copycols), x,
          makeunique=makeunique, copycols=copycols)

# Create Dataset
Base.hcat(ds1::Dataset, ds2::AbstractDataset;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(copy(ds1, copycols=copycols), ds2,
          makeunique=makeunique, copycols=copycols)

# Create Dataset
Base.hcat(ds1::Dataset, ds2::AbstractDataset, dsn::AbstractDataset...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(ds1, ds2, makeunique=makeunique, copycols=copycols), dsn...,
          makeunique=makeunique, copycols=copycols)

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
    f_col = getformat(ds, col)
    ds[!, col] = allowmissing(ds[!, col])
    setformat!(ds, col, f_col)
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
    f_col = getformat(ds, col)
    if !(!error && Missing <: eltype(x) && any(ismissing, x))
        ds[!, col] = disallowmissing(x)
        setformat!(ds, col, f_col)
    end
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
    append!(ds::Dataset, ds2::AbstractDataset; cols::Symbol=:setequal,
            promote::Bool=(cols in [:union, :subset]))
    append!(ds::Dataset, table; cols::Symbol=:setequal,
            promote::Bool=(cols in [:union, :subset]))

Add the rows of `ds2` to the end of `ds`. If the second argument `table` is not an
`AbstractDataset` then it is converted using `Dataset(table, copycols=false)`
before being appended.

The exact behavior of `append!` depends on the `cols` argument:
* If `cols == :setequal` (this is the default)
  then `ds2` must contain exactly the same columns as `ds` (but possibly in a
  different order).
* If `cols == :orderequal` then `ds2` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(ds)` to allow for support of ordered dicts; however, if `ds2`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `ds2` may contain more columns than `ds`, but all
  column names that are present in `ds` must be present in `ds2` and only these
  are used.
* If `cols == :subset` then `append!` behaves like for `:intersect` but if some
  column is missing in `ds2` then a `missing` value is pushed to `ds`.
* If `cols == :union` then `append!` adds columns missing in `ds` that are present
  in `ds2`, for columns present in `ds` but missing in `ds2` a `missing` value
  is pushed.

If `promote=true` and element type of a column present in `ds` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `ds`. If `promote=false` an error
is thrown.

The above rule has the following exceptions:
* If `ds` has no columns then copies of columns from `ds2` are added to it.
* If `ds2` has no columns then calling `append!` leaves `ds` unchanged.

Please note that `append!` must not be used on a `Dataset` that contains
columns that are aliases (equal when compared with `===`).

# See also

Use [`push!`](@ref) to add individual rows to a data set and [`vcat`](@ref)
to vertically concatenate data sets.

# Examples
```jldoctest
julia> ds1 = Dataset(A=1:3, B=1:3)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> ds2 = Dataset(A=4.0:6.0, B=4:6)
3×2 Dataset
 Row │ A        B
     │ Float64  Int64
─────┼────────────────
   1 │     4.0      4
   2 │     5.0      5
   3 │     6.0      6

julia> append!(ds1, ds2);

julia> ds1
6×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      5
   6 │     6      6
```
"""
function Base.append!(ds1::Dataset, ds2::AbstractDataset; cols::Symbol=:setequal,
                      promote::Bool=(cols in [:union, :subset]))
# appending ds2 to ds1 should keep the formats of ds1, and metadata of ds1, if ds1 is
# empty then formats of ds2 transfer to ds1, but not metadata
# Modify Dataset
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end

    if ncol(ds1) == 0
        for (n, v) in pairs(eachcol(ds2))
            f_col = getformat(ds2, n)
            ds1[!, n] = copy(v) # make sure ds1 does not reuse ds2
            setformat!(ds1, n => f_col)
        end
        return ds1
    end
    ncol(ds2) == 0 && return ds1

    if cols == :orderequal && _names(ds1) != _names(ds2)
        wrongnames = symdiff(_names(ds1), _names(ds2))
        if isempty(wrongnames)
            mismatches = findall(_names(ds1) .!= _names(ds2))
            @assert !isempty(mismatches)
            throw(ArgumentError("Columns number " *
                                join(mismatches, ", ", " and ") *
                                " do not have the same names in both passed " *
                                "data sets and `cols == :orderequal`"))
        else
            mismatchmsg = " Column names :" *
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data sets " *
                                "and `cols == :orderequal`"))
        end
    elseif cols == :setequal
        wrongnames = symdiff(_names(ds1), _names(ds2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data sets " *
                                "and `cols == :setequal`"))
        end
    elseif cols == :intersect
        wrongnames = setdiff(_names(ds1), _names(ds2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only in destination data set " *
                                "and `cols == :intersect`"))
        end
    end

    nrows, ncols = size(ds1)
    targetrows = nrows + nrow(ds2)
    current_col = 0
    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `Dataset` is internally
    # inconsistent and normal data set indexing would error.

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    ##### modify the code to take care of meta data
    try
        current_modified_time = _attributes(ds1).meta.modified[]
        for (j, n) in enumerate(_names(ds1))
            current_col += 1
            format_of_cur_col = getformat(ds1, n)
            if hasproperty(ds2, n)
                ds2_c = ds2[!, n]
                S = eltype(ds2_c)
                ds1_c = ds1[!, j]
                T = eltype(ds1_c)
                if S <: T || !promote || promote_type(S, T) <: T
                    # if S <: T || promote_type(S, T) <: T this should never throw an exception
                    append!(ds1_c, ds2_c)
                else
                    newcol = similar(ds1_c, promote_type(S, T), targetrows)
                    copyto!(newcol, 1, ds1_c, 1, nrows)
                    copyto!(newcol, nrows+1, ds2_c, 1, targetrows - nrows)
                    firstindex(newcol) != 1 && _onebased_check_error()
                    _columns(ds1)[j] = newcol
                end
            else
                if Missing <: eltype(ds1[!, j])
                    resize!(ds1[!, j], targetrows)
                    ds1[nrows+1:targetrows, j] .= missing
                elseif promote
                    newcol = similar(ds1[!, j], Union{Missing, eltype(ds1[!, j])},
                                     targetrows)
                    copyto!(newcol, 1, ds1[!, j], 1, nrows)
                    newcol[nrows+1:targetrows] .= missing
                    firstindex(newcol) != 1 && _onebased_check_error()
                    _columns(ds1)[j] = newcol
                else
                    throw(ArgumentError("promote=false and source data set does " *
                                        "not contain column :$n, while destination " *
                                        "column does not allow for missing values"))
                end
            end
            setformat!(ds1, n => format_of_cur_col)
            _modified(_attributes(ds1))
        end
        current_col = 0
        for col in _columns(ds1)
            current_col += 1
            @assert length(col) == targetrows
        end
        if cols == :union
            for n in setdiff(_names(ds2), _names(ds1))
                newcol = similar(ds2[!, n], Union{Missing, eltype(ds2[!, n])},
                                 targetrows)
                @inbounds newcol[1:nrows] .= missing
                copyto!(newcol, nrows+1, ds2[!, n], 1, targetrows - nrows)
                ds1[!, n] = newcol
                _modified(_attributes(ds1))
            end
        end
    catch err
        # Undo changes in case of error
        for col in _columns(ds1)
            resize!(col, nrows)
        end
        # go back to original modified time
        _attributes(ds1).meta.modified[] = current_modified_time
        @error "Error adding value to column :$(_names(ds1)[current_col])."
        rethrow(err)
    end
    return ds1
end

# TODO needs more works on how to handle formats??
# Modify Dataset
function Base.push!(ds::Dataset, row::Union{AbstractDict, NamedTuple};
                    cols::Symbol=:setequal,
                    promote::Bool=(cols in [:union, :subset]))
    # push keep formats
    possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
    if !(cols in possible_cols)
        throw(ArgumentError("`cols` keyword argument must be any of :" *
                            join(possible_cols, ", :")))
    end

    nrows, ncols = size(ds)
    targetrows = nrows + 1
    # here the formats should be kept, setproperty! modifies time
    if ncols == 0 && row isa NamedTuple
        for (n, v) in pairs(row)
            format_of_cur_col = getformat(ds, n)
            setproperty!(ds, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
            setformat!(ds, n => format_of_cur_col)
        end
        return ds
    end

    old_row_type = typeof(row)
    if row isa AbstractDict && keytype(row) !== Symbol &&
        (keytype(row) <: AbstractString || all(x -> x isa AbstractString, keys(row)))
        row = (;(Symbol.(keys(row)) .=> values(row))...)
    end

    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `Dataset` is internally
    # inconsistent and normal data set indexing would error.
    if cols == :union
        current_modified = _attributes(ds).meta.modified[]
        if row isa AbstractDict && keytype(row) !== Symbol && !all(x -> x isa Symbol, keys(row))
            throw(ArgumentError("when `cols == :union` all keys of row must be Symbol"))
        end
        for (i, colname) in enumerate(_names(ds))
            format_of_cur_col = getformat(ds, colname)
            col = _columns(ds)[i]
            if haskey(row, colname)
                val = row[colname]
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
                    setformat!(ds, colname => format_of_cur_col)
                    for col in _columns(ds)
                        resize!(col, nrows)
                    end
                    _attributes(ds).meta.modified[] = current_modified
                    @error "Error adding value to column :$colname."
                    rethrow(err)
                end
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(ds)[i] = newcol
                setformat!(ds, colname => format_of_cur_col)
                _modified(_attributes(ds))
            end
        end
        for (colname, col) in zip(_names(ds), _columns(ds))
            if length(col) != targetrows
                for col2 in _columns(ds)
                    resize!(col2, nrows)
                end
                _attributes(ds).meta.modified[] = current_modified
                throw(AssertionError("Error adding value to column :$colname"))
            end
        end
        for colname in setdiff(keys(row), _names(ds))
            val = row[colname]
            S = typeof(val)
            if nrows == 0
                newcol = [val]
            else
                newcol = Tables.allocatecolumn(Union{Missing, S}, targetrows)
                fill!(newcol, missing)
                newcol[end] = val
            end
            ds[!, colname] = newcol
        end
        return ds
    end

    if cols == :orderequal
        if old_row_type <: Dict
            throw(ArgumentError("passing `Dict` as `row` when `cols == :orderequal` " *
                                "is not allowed as it is unordered"))
        elseif length(row) != ncol(ds) || any(x -> x[1] != x[2], zip(keys(row), _names(ds)))
            throw(ArgumentError("when `cols == :orderequal` pushed row must " *
                                "have the same column names and in the " *
                                "same order as the target data set"))
        end
    elseif cols === :setequal
        # Only check for equal lengths if :setequal is selected,
        # as an error will be thrown below if some names don't match
        if length(row) != ncols
            # an explicit error is thrown as this was allowed in the past
            throw(ArgumentError("`push!` with `cols` equal to `:setequal` " *
                                "requires `row` to have the same number of elements " *
                                "as the number of columns in `ds`."))
        end
    end
    current_col = 0
    try
        current_modified = _attributes(ds).meta.modified[]
        for (col, nm) in zip(_columns(ds), _names(ds))
            format_of_cur_col = getformat(ds, nm)
            current_col += 1
            if cols === :subset
                val = get(row, nm, missing)
            else
                val = row[nm]
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
                _columns(ds)[columnindex(ds, nm)] = newcol
                setformat!(ds, nm => format_of_cur_col)
            end
        end
        current_col = 0
        for col in _columns(ds)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        for col in _columns(ds)
            resize!(col, nrows)
        end
        _attributes(ds).meta.modified[] = current_modified
        @error "Error adding value to column :$(_names(ds)[current_col])."
        rethrow(err)
    end
    return ds
end

"""
    push!(ds::Dataset, row::Union{Tuple, AbstractArray}; promote::Bool=false)
    push!(ds::Dataset, row::Union{DatasetRow, NamedTuple, AbstractDict};
          cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset]))

Add in-place one row at the end of `ds` taking the values from `row`.

Column types of `ds` are preserved, and new values are converted if necessary.
An error is thrown if conversion fails.

If `row` is neither a `DatasetRow`, `NamedTuple` nor `AbstractDict` then
it must be a `Tuple` or an `AbstractArray`
and columns are matched by order of appearance. In this case `row` must contain
the same number of elements as the number of columns in `ds`.

If `row` is a `DatasetRow`, `NamedTuple` or `AbstractDict` then
values in `row` are matched to columns in `ds` based on names. The exact behavior
depends on the `cols` argument value in the following way:
* If `cols == :setequal` (this is the default)
  then `row` must contain exactly the same columns as `ds` (but possibly in a
  different order).
* If `cols == :orderequal` then `row` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(ds)` to allow for support of ordered dicts; however, if `row`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `row` may contain more columns than `ds`,
  but all column names that are present in `ds` must be present in `row` and only
  they are used to populate a new row in `ds`.
* If `cols == :subset` then `push!` behaves like for `:intersect` but if some
  column is missing in `row` then a `missing` value is pushed to `ds`.
* If `cols == :union` then columns missing in `ds` that are present in `row` are
  added to `ds` (using `missing` for existing rows) and a `missing` value is
  pushed to columns missing in `row` that are present in `ds`.

If `promote=true` and element type of a column present in `ds` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `ds`. If `promote=false` an error
is thrown.

As a special case, if `ds` has no columns and `row` is a `NamedTuple` or
`DatasetRow`, columns are created for all values in `row`, using their names
and order.

Please note that `push!` must not be used on a `Dataset` that contains columns
that are aliases (equal when compared with `===`).

# Examples
```jldoctest
julia> ds = Dataset(A=1:3, B=1:3);

julia> push!(ds, (true, false))
4×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0

julia> push!(ds, ds[1, :])
5×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0
   5 │     1      1

julia> push!(ds, (C="something", A=true, B=false), cols=:intersect)
6×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0
   5 │     1      1
   6 │     1      0

julia> push!(ds, Dict(:A=>1.0, :C=>1.0), cols=:union)
7×3 Dataset
 Row │ A        B        C
     │ Float64  Int64?   Float64?
─────┼─────────────────────────────
   1 │     1.0        1  missing
   2 │     2.0        2  missing
   3 │     3.0        3  missing
   4 │     1.0        0  missing
   5 │     1.0        1  missing
   6 │     1.0        0  missing
   7 │     1.0  missing        1.0

julia> push!(ds, NamedTuple(), cols=:subset)
8×3 Dataset
 Row │ A          B        C
     │ Float64?   Int64?   Float64?
─────┼───────────────────────────────
   1 │       1.0        1  missing
   2 │       2.0        2  missing
   3 │       3.0        3  missing
   4 │       1.0        0  missing
   5 │       1.0        1  missing
   6 │       1.0        0  missing
   7 │       1.0  missing        1.0
   8 │ missing    missing  missing
```
"""
function Base.push!(ds::Dataset, row::Any; promote::Bool=false)

# Modify Dataset
    if !(row isa Union{Tuple, AbstractArray})
        # an explicit error is thrown as this was allowed in the past
        throw(ArgumentError("`push!` does not allow passing collections of type " *
                            "$(typeof(row)) to be pushed into a Dataset. Only " *
                            "`Tuple`, `AbstractArray`, `AbstractDict`, `DatasetRow` " *
                            "and `NamedTuple` are allowed."))
    end
    nrows, ncols = size(ds)
    targetrows = nrows + 1
    if length(row) != ncols
        msg = "Length of `row` does not match `Dataset` column count."
        throw(DimensionMismatch(msg))
    end
    current_col = 0
    try
        current_modified = _attributes(ds).meta.modified[]
        for (i, (col, val)) in enumerate(zip(_columns(ds), row))
            format_of_cur_col = getformat(ds, col)
            current_col += 1
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                push!(col, val)
            else
                newcol = Tables.allocatecolumn(promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(ds)[i] = newcol
                setformat!(ds, i => format_of_cur_col)
                _modified(_attributes(ds))
            end
        end
        current_col = 0
        for col in _columns(ds)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        #clean up partial row
        for col in _columns(ds)
            resize!(col, nrows)
        end
        _attributes(ds).meta.modified[] = current_modified
        @error "Error adding value to column :$(_names(ds)[current_col])."
        rethrow(err)
    end
    ds
end

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
    inner < 0 && throw(ArgumentError("inner keyword argument must be non-negative"))
    outer < 0 && throw(ArgumentError("outer keyword argument must be non-negative"))
    return mapcols!(x -> repeat(x, inner = Int(inner), outer = Int(outer)), ds)
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
    count < 0 && throw(ArgumentError("count must be non-negative"))
    return mapcols!(x -> repeat(x, Int(count)), ds)
end

# This is not exactly copy! as in general we allow axes to be different

# Modify Dataset
function _replace_columns!(ds::Dataset, newds::Dataset)
    copy!(_columns(ds), _columns(newds))
    copy!(_names(index(ds)), _names(newds))
    copy!(index(ds).lookup, index(newds).lookup)
    copy!(index(ds).format, index(newds).format)
    # TODO should info also be transferred to ds ???
    _modified(_attributes(ds))
    return ds
end
