Tables.istable(::Type{<:AbstractDataset}) = true
Tables.columnaccess(::Type{<:AbstractDataset}) = true
Tables.columns(df::AbstractDataset) = eachcol(df)
# TODO Monitor this for any unseen problem
Tables.rowaccess(::Type{<:AbstractDataset}) = false
Tables.rows(df::AbstractDataset) = eachrow(df)
Tables.rowtable(df::AbstractDataset) = Tables.rowtable(Tables.columntable(df))
Tables.namedtupleiterator(df::AbstractDataset) =
    Tables.namedtupleiterator(Tables.columntable(df))

function Tables.columnindex(df::Union{AbstractDataset, DatasetRow}, idx::Symbol)
    ind = index(df)
    if ind isa Index
        return get(ind.lookup, idx, 0)
    else
        parent_ind = ind.parent
        loc = get(parent_ind.lookup, idx, 0)
        return loc == 0 || loc > length(ind.remap) ? 0 : max(0, ind.remap[loc])
    end
end

Tables.columnindex(df::Union{AbstractDataset, DatasetRow}, idx::AbstractString) =
    columnindex(df, Symbol(idx))

Tables.schema(df::AbstractDataset) = Tables.Schema(_names(df), [eltype(col) for col in eachcol(df)])
Tables.materializer(df::AbstractDataset) = Dataset

Tables.getcolumn(ds::Dataset, i::Int) = ds[!, i]
Tables.getcolumn(sds::SubDataset, i::Int) = view(_columns(parent(sds))[i], rows(sds))
Tables.getcolumn(ds::Dataset, nm::Symbol) = ds[!, nm]

function Tables.getcolumn(sds::SubDataset, nm::Symbol)
    i = lookupname(parent(index(sds)).lookup, nm)
    view(_columns(parent(sds))[i], rows(sds))
end

Tables.getcolumn(dfr::DatasetRow, i::Int) = dfr[i]
Tables.getcolumn(dfr::DatasetRow, nm::Symbol) = dfr[nm]

getvector(x::AbstractVector) = x
getvector(x) = [x[i] for i = 1:length(x)]

fromcolumns(x, names; copycols::Union{Nothing, Bool}=nothing) =
    Dataset(AbstractVector[getvector(Tables.getcolumn(x, nm)) for nm in names],
              Index(names),
              copycols=something(copycols, true))

# note that copycols is false by default in this definition (Tables.CopiedColumns
# implies copies have already been made) but if `copycols=true`, a copy will still be
# made; this is useful for scenarios where the input is immutable so avoiding copies
# is desirable, but you may still want a copy for mutation (Arrow.Table is like this)
fromcolumns(x::Tables.CopiedColumns, names; copycols::Union{Nothing, Bool}=nothing) =
    fromcolumns(Tables.source(x), names; copycols=something(copycols, false))

function Dataset(x::T; copycols::Union{Nothing, Bool}=nothing) where {T}
    if !Tables.istable(x) && x isa AbstractVector && !isempty(x)
        # here we handle eltypes not specific enough to be dispatched
        # to other Datasets constructors taking vector of `Pair`s
        if all(v -> v isa Pair{Symbol, <:AbstractVector}, x) ||
            all(v -> v isa Pair{<:AbstractString, <:AbstractVector}, x)
            return Dataset(AbstractVector[last(v) for v in x], [first(v) for v in x],
                             copycols=something(copycols, true))
        end
    end
    cols = Tables.columns(x)
    names = collect(Symbol, Tables.columnnames(cols))
    return fromcolumns(cols, names, copycols=copycols)
end

function Base.append!(df::Dataset, table; cols::Symbol=:setequal,
                      promote::Bool=(cols in [:union, :subset]))
    if table isa Dict && cols == :orderequal
        throw(ArgumentError("passing `Dict` as `table` when `cols` is equal to " *
                            "`:orderequal` is not allowed as it is unordered"))
    end
    append!(df, Dataset(table, copycols=false), cols=cols, promote=promote)
end

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
Dataset(x::AbstractVector{NamedTuple{names, T}}; copycols::Bool=true) where {names, T} =
    fromcolumns(Tables.columns(Tables.IteratorWrapper(x)), collect(names), copycols=false)

Tables.istable(::Type{<:Union{DatasetRows, DatasetColumns}}) = true
Tables.columnaccess(::Type{<:Union{DatasetRows, DatasetColumns}}) = true
Tables.rowaccess(::Type{<:Union{DatasetRows, DatasetColumns}}) = true
Tables.columns(itr::Union{DatasetRows, DatasetColumns}) = Tables.columns(parent(itr))
Tables.rows(itr::Union{DatasetRows, DatasetColumns}) = Tables.rows(parent(itr))
Tables.schema(itr::Union{DatasetRows, DatasetColumns}) = Tables.schema(parent(itr))
Tables.rowtable(itr::Union{DatasetRows, DatasetColumns}) = Tables.rowtable(parent(itr))
Tables.namedtupleiterator(itr::Union{DatasetRows, DatasetColumns}) =
    Tables.namedtupleiterator(parent(itr))
Tables.materializer(itr::Union{DatasetRows, DatasetColumns}) =
    Tables.materializer(parent(itr))

Tables.getcolumn(itr::Union{DatasetRows, DatasetColumns}, i::Int) =
    Tables.getcolumn(parent(itr), i)
Tables.getcolumn(itr::Union{DatasetRows, DatasetColumns}, nm::Symbol) =
    Tables.getcolumn(parent(itr), nm)

IteratorInterfaceExtensions.getiterator(df::AbstractDataset) =
    Tables.datavaluerows(Tables.columntable(df))
IteratorInterfaceExtensions.isiterable(x::AbstractDataset) = true
TableTraits.isiterabletable(x::AbstractDataset) = true
