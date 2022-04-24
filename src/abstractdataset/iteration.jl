##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# Iteration by rows
"""
    DatasetRows{D<:AbstractDataset} <: AbstractVector{DatasetRow}

Iterator over rows of an `AbstractDataset`,
with each row represented as a `DatasetRow`.

A value of this type is returned by the [`eachrow`](@ref) function.
"""

struct DatasetRows{D<:AbstractDataset} <: AbstractVector{DatasetRow}
    df::D
end
#
Base.summary(dfrs::DatasetRows) = "$(length(dfrs))-element DatasetRows"
Base.summary(io::IO, dfrs::DatasetRows) = print(io, summary(dfrs))

Base.iterate(::AbstractDataset) =
    error("AbstractDataset is not iterable.")

"""
    eachrow(df::AbstractDataset)

Return a `DatasetRows` that iterates a data set row by row,
with each row represented as a `DatasetRow`.

Because `DatasetRow`s have an `eltype` of `Any`, use `copy(dfr::DatasetRow)` to obtain
a named tuple, which supports iteration and property access like a `DatasetRow`,
but also passes information on the `eltypes` of the columns of `df`.

# Examples
```jldoctest
julia> df = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> eachrow(df)
4×2 DatasetRows
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> copy.(eachrow(df))
4-element Vector{NamedTuple{(:x, :y), Tuple{Int64, Int64}}}:
 (x = 1, y = 11)
 (x = 2, y = 12)
 (x = 3, y = 13)
 (x = 4, y = 14)

julia> eachrow(view(df, [4, 3], [2, 1]))
2×2 DatasetRows
 Row │ y      x
     │ Int64  Int64
─────┼──────────────
   1 │    14      4
   2 │    13      3
```
"""
function Base.eachrow(df::AbstractDataset)
    DatasetRows(df)
end

Base.IndexStyle(::Type{<:DatasetRows}) = Base.IndexLinear()
Base.size(itr::DatasetRows) = (size(parent(itr), 1), )

Base.@propagate_inbounds Base.getindex(itr::DatasetRows, i::Int) = parent(itr)[i, :]

# separate methods are needed due to dispatch ambiguity
Base.getproperty(itr::DatasetRows, col_ind::Symbol) =
    getproperty(parent(itr), col_ind)
Base.getproperty(itr::DatasetRows, col_ind::AbstractString) =
    getproperty(parent(itr), col_ind)
Compat.hasproperty(itr::DatasetRows, s::Symbol) = haskey(index(parent(itr)), s)
Compat.hasproperty(itr::DatasetRows, s::AbstractString) = haskey(index(parent(itr)), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DatasetRows, private::Bool=false) = propertynames(parent(itr))


# iteration by group

struct GroupedDataset{D<:Union{Dataset, GroupBy, GatherBy}}
    ds::D
end

Base.summary(gds::GroupedDataset) = "$(size(gds)[1])-element grouped data set"
Base.summary(io::IO, gds::GroupedDataset) = print(io, summary(gds))

function eachgroup(ds::Dataset)
    !isgrouped(ds) && throw(ArgumentError("The data set is not grouped"))
    GroupedDataset(ds)
end
function eachgroup(ds::Union{GroupBy, GatherBy})
    GroupedDataset(ds)
end


Base.IndexStyle(::Type{<:GroupedDataset}) = Base.IndexLinear()
Base.size(itr::GroupedDataset{Dataset}) = (index(itr.ds).ngroups[], )
Base.size(itr::GroupedDataset{<:Union{GroupBy, GatherBy}}) = (itr.ds.lastvalid, )

function Base.getindex(itr::GroupedDataset{Dataset}, i::Int)
    i > size(itr)[1] && throw(BoundsError(itr, i))
    st = index(itr.ds).starts
    i == size(itr)[1] ? hi = nrow(itr.ds) : hi = st[i+1]-1
    lo = st[i]
    view(itr.ds, lo:hi, :)
end
function Base.getindex(itr::GroupedDataset{<:Union{GroupBy, GatherBy}}, i::Int)
    i > size(itr)[1] && throw(BoundsError(itr, i))
    st = _group_starts(itr.ds)
    prm = _get_perms(itr.ds)
    i == size(itr)[1] ? hi = nrow(parent(itr.ds)) : hi = st[i+1]-1
    lo = st[i]
    view(parent(itr.ds), view(prm, lo:hi), :)
end

# Iteration by columns

const DATASETCOLUMNS_DOCSTR = """
Indexing into `DatasetColumns` objects using integer, `Symbol` or string
returns the corresponding column (without copying).
Indexing into `DatasetColumns` objects using a multiple column selector
returns a subsetted `DatasetColumns` object with a new parent containing
only the selected columns (without copying).

`DatasetColumns` supports most of the `AbstractVector` API. The key
differences are that it is read-only and that the `keys` function returns a
vector of `Symbol`s (and not integers as for normal vectors).

In particular `findnext`, `findprev`, `findfirst`, `findlast`, and `findall`
functions are supported, and in `findnext` and `findprev` functions it is allowed
to pass an integer, string, or `Symbol` as a reference index.
"""

"""
    DatasetColumns{<:AbstractDataset}

A vector-like object that allows iteration over columns of an `AbstractDataset`.

$DATASETCOLUMNS_DOCSTR
"""
struct DatasetColumns{T<:AbstractDataset}
    df::T
end

Base.summary(dfcs::DatasetColumns)= "$(length(dfcs))-element DatasetColumns"
Base.summary(io::IO, dfcs::DatasetColumns) = print(io, summary(dfcs))

"""
    eachcol(df::AbstractDataset)

Return a `DatasetColumns` object that is a vector-like that allows iterating
an `AbstractDataset` column by column.

```
"""
Base.eachcol(ds::AbstractDataset) = DatasetColumns(ds)

Base.IteratorSize(::Type{<:DatasetColumns}) = Base.HasShape{1}()
Base.size(itr::DatasetColumns) = (size(parent(itr), 2),)

function Base.size(itr::DatasetColumns, d::Integer)
    d != 1 && throw(ArgumentError("dimension out of range"))
    return size(itr)[1]
end

Base.ndims(::DatasetColumns) = 1
Base.ndims(::Type{<:DatasetColumns}) = 1

Base.length(itr::DatasetColumns) = size(itr)[1]
Base.eltype(::Type{<:DatasetColumns}) = AbstractVector

Base.firstindex(itr::DatasetColumns) = 1
Base.lastindex(itr::DatasetColumns) = length(itr)

if VERSION < v"1.6"
    Base.firstindex(itr::DatasetColumns, i::Integer) = first(axes(itr, i))
    Base.lastindex(itr::DatasetColumns, i::Integer) = last(axes(itr, i))
end
Base.axes(itr::DatasetColumns, i::Integer) = Base.OneTo(size(itr, i))

Base.iterate(itr::DatasetColumns, i::Integer=1) =
    i <= length(itr) ? (itr[i], i + 1) : nothing

# FIXME this needs fixing for SubDataset, because part of ds is out there for modifying without tracking
function Base.getindex(itr::DatasetColumns, idx::ColumnIndex)
    parent(itr)[!, idx]
end
Base.@propagate_inbounds Base.getindex(itr::DatasetColumns, idx::MultiColumnIndex) =
    eachcol(parent(itr)[!, idx])
Base.:(==)(itr1::DatasetColumns, itr2::DatasetColumns) =
    parent(itr1) == parent(itr2)
Base.isequal(itr1::DatasetColumns, itr2::DatasetColumns) =
    isequal(parent(itr1), parent(itr2))

# separate methods are needed due to dispatch ambiguity
Base.getproperty(itr::DatasetColumns, col_ind::Symbol) =
    getproperty(parent(itr), col_ind)
Base.getproperty(itr::DatasetColumns, col_ind::AbstractString) =
    getproperty(parent(itr), col_ind)
Compat.hasproperty(itr::DatasetColumns, s::Symbol) =
    haskey(index(parent(itr)), s)
Compat.hasproperty(itr::DatasetColumns, s::AbstractString) =
    haskey(index(parent(itr)), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DatasetColumns, private::Bool=false) =
    propertynames(parent(itr))

"""
    keys(dfc::DatasetColumns)

Get a vector of column names of `dfc` as `Symbol`s.
"""
Base.keys(itr::DatasetColumns) = propertynames(itr)

"""
    values(dfc::DatasetColumns)

Get a vector of columns from `dfc`.
"""
Base.values(itr::DatasetColumns) = collect(itr)

"""
    pairs(dfc::DatasetColumns)

Return an iterator of pairs associating the name of each column of `dfc`
with the corresponding column vector, i.e. `name => col`
where `name` is the column name of the column `col`.
"""
Base.pairs(itr::DatasetColumns) = Base.Iterators.Pairs(itr, keys(itr))
Base.findnext(f::Function, itr::DatasetColumns, i::Integer) =
    findnext(f, values(itr), i)
Base.findnext(f::Function, itr::DatasetColumns, i::Union{Symbol, AbstractString}) =
    findnext(f, values(itr), index(parent(itr))[i])
Base.findprev(f::Function, itr::DatasetColumns, i::Integer) =
    findprev(f, values(itr), i)
Base.findprev(f::Function, itr::DatasetColumns, i::Union{Symbol, AbstractString}) =
    findprev(f, values(itr), index(parent(itr))[i])
Base.findfirst(f::Function, itr::DatasetColumns) =
    findfirst(f, values(itr))
Base.findlast(f::Function, itr::DatasetColumns) =
    findlast(f, values(itr))
Base.findall(f::Function, itr::DatasetColumns) =
    findall(f, values(itr))
#
Base.parent(itr::DatasetColumns) = getfield(itr, :df)
Base.parent(itr::DatasetRows) = getfield(itr, :df)

Base.names(itr::DatasetColumns) = names(parent(itr))
Base.names(itr::DatasetColumns, cols) = names(parent(itr), cols)

function Base.show(io::IO, dfrs::DatasetRows;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   eltypes::Bool = true,
                   truncate::Int = 32,
                   kwargs...)
    df = parent(dfrs)
    title = summary ? "$(nrow(df))×$(ncol(df)) DatasetRows" : ""
    _show(io, df; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
          summary=false, eltypes=eltypes, truncate=truncate, title=title,
          kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", dfrs::DatasetRows;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(io, dfrs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

Base.show(dfrs::DatasetRows;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(stdout, dfrs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

function Base.show(io::IO, dfcs::DatasetColumns;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   eltypes::Bool = true,
                   truncate::Int = 32,
                   kwargs...)
    df = parent(dfcs)
    title = summary ? "$(nrow(df))×$(ncol(df)) DatasetColumns" : ""
    _show(io, parent(dfcs); allrows=allrows, allcols=allcols, rowlabel=rowlabel,
          summary=false, eltypes=eltypes, truncate=truncate, title=title,
          kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", dfcs::DatasetColumns;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(io, dfcs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

Base.show(dfcs::DatasetColumns;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(stdout, dfcs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)




# prevent using broadcasting to mutate columns e.g. in pop!.(eachcol(ds))
Base.broadcasted(::typeof(pop!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `pop!` over DatasetColums is reserved."))
Base.broadcasted(::typeof(popfirst!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `popfirst!` over DatasetColums is reserved."))
Base.broadcasted(::typeof(pushfirst!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `pushfirst!` over DatasetColums is reserved."))
Base.broadcasted(::typeof(fill!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `fill!` over DatasetColums is reserved."))
Base.broadcasted(::typeof(Statistics.median!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `median!` over DatasetColums is reserved."))
Base.broadcasted(::typeof(sort!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `sort!` over DatasetColums is reserved."))
Base.broadcasted(::typeof(lag!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `lag!` over DatasetColums is reserved."))
Base.broadcasted(::typeof(lead!), ::DatasetColumns, args...) = throw(ArgumentError("broadcasting `lead!` over DatasetColums is reserved."))
