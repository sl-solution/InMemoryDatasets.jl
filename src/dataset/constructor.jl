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
            return new(AbstractVector[], Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}(), Int[], Int[], false, colindex.perm, colindex.starts, 1, false), Attributes())
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
        return allowmissing(collect(col))
    elseif col isa AbstractVector
        if isa(col, BitVector)
            return convert(Vector{Union{Bool, Missing}}, col)
        else
            _res = allowmissing(col)
            if copycols
                _res === col ? copy(_res) : _res
            else
                _res
            end
        end
    elseif col isa Union{AbstractArray{<:Any, 0}, Ref}
        x = col[]
        return fill!(allocatecol(Union{Missing, typeof(x)}, len), x)
    elseif col isa AbstractArray
        throw(ArgumentError("adding AbstractArray other than AbstractVector " *
                            "as a column of a data set is not allowed"))
    else
        return fill!(allocatecol(Union{Missing, typeof(col)}, len), col)
    end
end

# Create Dataset
Dataset(df::Dataset) = copy(df)

# Create Dataset
function Dataset(pairs::Pair{Symbol, <:Any}...; makeunique::Bool=false,
                   )::Dataset
    colnames = [Symbol(k) for (k, v) in pairs]
    columns = Any[v for (k, v) in pairs]
    return Dataset(columns, Index(colnames, makeunique=makeunique)
                     )
end

# Create Dataset
function Dataset(pairs::Pair{<:AbstractString, <:Any}...; makeunique::Bool=false)::Dataset
    colnames = [Symbol(k) for (k, v) in pairs]
    columns = Any[v for (k, v) in pairs]
    return Dataset(columns, Index(colnames, makeunique=makeunique))
end


# Create Dataset
# this is needed as a workaround for Tables.jl dispatch
function Dataset(pairs::AbstractVector{<:Pair}; makeunique::Bool=false)
    if isempty(pairs)
        return Dataset()
    else
        if !(all(((k, v),) -> k isa Symbol, pairs) || all(((k, v),) -> k isa AbstractString, pairs))
            throw(ArgumentError("All column names must be either Symbols or strings (mixing is not allowed)"))
        end
        colnames = [Symbol(k) for (k, v) in pairs]
        columns = Any[v for (k, v) in pairs]
        return Dataset(columns, Index(colnames, makeunique=makeunique))
    end
end

# Create Dataset
function Dataset(d::AbstractDict)
    if all(k -> k isa Symbol, keys(d))
        colnames = collect(Symbol, keys(d))
    elseif all(k -> k isa AbstractString, keys(d))
        colnames = [Symbol(k) for k in keys(d)]
    else
        throw(ArgumentError("All column names must be either Symbols or strings (mixing is not allowed)"))
    end

    colindex = Index(colnames)
    columns = Any[v for v in values(d)]
    df = Dataset(columns, colindex)
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


"""
    copy(ds::Dataset; copycols::Bool=true)

Copy data set `ds`.
If `copycols=true` (the default), return a new  `Dataset` holding
copies of column vectors in `ds`.
If `copycols=false`, return a new `Dataset` sharing column vectors with `ds`.

> This function uses `copy` rather than `deepcopy` internally, thus, it is not safe to use it when observations are mutable.
"""
function Base.copy(ds::Dataset)
    # TODO currently if the observation is mutable, copying data set doesn't protect it
    # Create Dataset
    newds = Dataset(copy(_columns(ds)), copy(index(ds)))
    setinfo!(newds, _attributes(ds).meta.info[])
    return newds
end
