"""
    AbstractDataset

An abstract type for which all concrete types expose an interface
for working with tabular data.

# Common methods

An `AbstractDataset` is a two-dimensional table with `Symbol`s or strings
for column names.

The following are normally implemented for AbstractDatasets:

* [`describe`](@ref) : summarize columns
* `summary` : show number of rows and columns
* `hcat` : horizontal concatenation
* `vcat` : vertical concatenation
* [`repeat`](@ref) : repeat rows
* `names` : columns names
* [`rename!`](@ref) : rename columns names based on keyword arguments
* `length` : number of columns
* `size` : (nrows, ncols)
* [`first`](@ref) : first `n` rows
* [`last`](@ref) : last `n` rows
* `convert` : convert to an array
* [`completecases`](@ref) : boolean vector of complete cases (rows with no missings)
* [`dropmissing`](@ref) : remove rows with missing values
* [`dropmissing!`](@ref) : remove rows with missing values in-place
* [`nonunique`](@ref) : indexes of duplicate rows
* [`unique`](@ref) : remove duplicate rows
* [`unique!`](@ref) : remove duplicate rows in-place
* [`disallowmissing`](@ref) : drop support for missing values in columns
* [`disallowmissing!`](@ref) : drop support for missing values in columns in-place
* [`allowmissing`](@ref) : add support for missing values in columns
* [`allowmissing!`](@ref) : add support for missing values in columns in-place
* `similar` : a Dataset with similar columns as `d`
* `filter` : remove rows
* `filter!` : remove rows in-place

# Indexing and broadcasting

`AbstractDataset` can be indexed by passing two indices specifying
row and column selectors. The allowed indices are a superset of indices
that can be used for standard arrays. You can also access a single column
of an `AbstractDataset` using `getproperty` and `setproperty!` functions.
Columns can be selected using integers, `Symbol`s, or strings.
In broadcasting `AbstractDataset` behavior is similar to a `Matrix`.

A detailed description of `getindex`, `setindex!`, `getproperty`, `setproperty!`,

"""
abstract type AbstractDataset end

##############################################################################
##
## Basic properties of a Dataset
##
##############################################################################

"""
    names(df::AbstractDataset)
    names(df::AbstractDataset, cols)

Return a freshly allocated `Vector{String}` of names of columns contained in `df`.

If `cols` is passed then restrict returned column names to those matching the
selector (this is useful in particular with regular expressions, `Cols`, `Not`, and `Between`).
`cols` can be:
* any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR)
* a `Type`, in which case names of columns whose `eltype` is a subtype of `T`
  are returned
* a `Function` predicate taking the column name as a string and returning `true`
  for columns that should be kept

See also [`propertynames`](@ref) which returns a `Vector{Symbol}`.
"""
Base.names(df::AbstractDataset, cols::Colon=:) = names(index(df))

function Base.names(df::AbstractDataset, cols)
    nms = _names(index(df))
    idx = index(df)[cols]
    idxs = idx isa Int ? (idx:idx) : idx
    return [String(nms[i]) for i in idxs]
end

Base.names(df::AbstractDataset, T::Type) =
    [String(n) for (n, c) in pairs(eachcol(df)) if eltype(c) <: T]
Base.names(df::AbstractDataset, fun::Function) = filter!(fun, names(df))

# _names returns Vector{Symbol} without copying
_names(df::AbstractDataset) = _names(index(df))

_getformats(ds::AbstractDataset) = index(ds).format

# separate methods are needed due to dispatch ambiguity
Compat.hasproperty(df::AbstractDataset, s::Symbol) = haskey(index(df), s)
Compat.hasproperty(df::AbstractDataset, s::AbstractString) = haskey(index(df), s)


##############################################################################
##
## getting, setting and removing formats
##
##############################################################################
function getformat(ds::AbstractDataset, idx::Integer)
    getformat(index(ds), idx)
end
function getformat(ds::AbstractDataset, y::Symbol)
    getformat(index(ds), y)
end
function getformat(ds::AbstractDataset, y::String)
    getformat(index(ds), y)
end

#Modify Dataset
function setformat!(ds::AbstractDataset, idx::Integer, f::Function)
    setformat!(index(ds), idx, f)
    _modified(_attributes(ds))
    ds
end
function setformat!(ds::AbstractDataset, idx::Symbol, f::Function)
    setformat!(index(ds), idx, f)
    _modified(_attributes(ds))
    ds
end
function setformat!(ds::AbstractDataset, idx::T, f::Function) where T <: AbstractString
    setformat!(index(ds), idx, f)
    _modified(_attributes(ds))
    ds
end
function setformat!(ds::AbstractDataset, p::Pair{Int64, T}) where T <: Function
   setformat!(index(ds), p)
   _modified(_attributes(ds))
   ds
end
function setformat!(ds::AbstractDataset, p::Pair{Symbol, T}) where T <: Function
    setformat!(index(ds), p)
    _modified(_attributes(ds))
    ds
end
function setformat!(ds::AbstractDataset, p::Pair{S, T}) where S <: AbstractString where T <: Function
    setformat!(index(ds), p)
    _modified(_attributes(ds))
    ds
end
function setformat!(ds::AbstractDataset, p::Pair{MC, T}) where T <: Function where MC <: MultiColumnIndex
    idx = index(ds)[p.first]
    for i in 1:length(idx)
        setformat!(index(ds), idx[i], p.second)
        # if for any reason one of the formatting is not working, we make sure the modified is correct
        _modified(_attributes(ds))
    end
    ds
end
function setformat!(ds::AbstractDataset, dict::Dict)
    for (k, v) in dict
        setformat!(ds, k, v)
    end
    ds
end
# TODO should we allowed arbitarary combination of Pair
function setformat!(ds::AbstractDataset, pv::Vector)
    for p in pv
        setformat!(index(ds), p)
        # if for any reason one of the formatting is not working, we make sure the modified is correct
        _modified(_attributes(ds))
    end
    ds
end
function setformat!(ds::AbstractDataset, args...)
    for i in 1:length(args)
        setformat!(index(ds), args[i])
    end
    ds
end
# removing formats
function removeformat!(ds::AbstractDataset, idx::Integer)
    removeformat!(index(ds), idx)
    _modified(_attributes(ds))
    ds
end
function removeformat!(ds::AbstractDataset, y::Symbol)
    removeformat!(index(ds), y)
    _modified(_attributes(ds))
    ds
end
function removeformat!(ds::AbstractDataset, y::String)
    removeformat!(index(ds), y)
    _modified(_attributes(ds))
    ds
end
function removeformat!(ds::AbstractDataset, y::UnitRange)
    removeformat!(index(ds), y)
    _modified(_attributes(ds))
    ds
end
function removeformat!(ds::AbstractDataset, cols::MultiColumnIndex)
    idx = index(ds)[cols]
    for i in 1:length(idx)
        removeformat!(ds, idx[i])
        _modified(_attributes(ds))
    end
    ds
end
removeformat!(ds::AbstractDataset) = removeformat!(ds, :)
function removeformat!(ds::AbstractDataset, args...)
    for i in 1:length(args)
        removeformat!(ds, args[i])
    end
    ds
end

# set info
function setinfo!(ds::AbstractDataset, s::String)
    _attributes(ds).meta.info[] = s
    _modified(_attributes(ds))
    nothing
end

# TODO needs better printing
function content(ds::AbstractDataset)
    println(summary(ds))
    println(" Created: ", _attributes(ds).meta.created)
    println("Modified: ", _attributes(ds).meta.modified[])
    println("    Info: ", _attributes(ds).meta.info[])
    f_v = Dict{Symbol, Function}()
    for (k, v) in index(ds).format
        push!(f_v, _names(ds)[k]=>v)
    end
    println(" Formats: ")
    f_v
end

"""
    rename!(df::AbstractDataset, vals::AbstractVector{Symbol};
            makeunique::Bool=false)
    rename!(df::AbstractDataset, vals::AbstractVector{<:AbstractString};
            makeunique::Bool=false)
    rename!(df::AbstractDataset, (from => to)::Pair...)
    rename!(df::AbstractDataset, d::AbstractDict)
    rename!(df::AbstractDataset, d::AbstractVector{<:Pair})
    rename!(f::Function, df::AbstractDataset)

Rename columns of `df` in-place.
Each name is changed at most once. Permutation of names is allowed.

# Arguments
- `df` : the `AbstractDataset`
- `d` : an `AbstractDict` or an `AbstractVector` of `Pair`s that maps
  the original names or column numbers to new names
- `f` : a function which for each column takes the old name as a `String`
  and returns the new name that gets converted to a `Symbol`
- `vals` : new column names as a vector of `Symbol`s or `AbstractString`s
  of the same length as the number of columns in `df`
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

If pairs are passed to `rename!` (as positional arguments or in a dictionary or
a vector) then:
* `from` value can be a `Symbol`, an `AbstractString` or an `Integer`;
* `to` value can be a `Symbol` or an `AbstractString`.

Mixing symbols and strings in `to` and `from` is not allowed.

See also: [`rename`](@ref)

# Examples
```jldoctest
julia> df = Dataset(i = 1, x = 2, y = 3)
1×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(df, Dict(:i => "A", :x => "X"))
1×3 Dataset
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(df, [:a, :b, :c])
1×3 Dataset
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(df, [:a, :b, :a])
ERROR: ArgumentError: Duplicate variable names: :a. Pass makeunique=true to make them unique using a suffix automatically.

julia> rename!(df, [:a, :b, :a], makeunique=true)
1×3 Dataset
 Row │ a      b      a_1
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(uppercase, df)
1×3 Dataset
 Row │ A      B      A_1
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3
```
"""
function rename!(ds::AbstractDataset, vals::AbstractVector{Symbol};
                 makeunique::Bool=false)
    # Modify Dataset
    rename!(index(ds), vals, makeunique=makeunique)
    _modified(_attributes(ds))
    return ds
end

function rename!(ds::AbstractDataset, vals::AbstractVector{<:AbstractString};
                 makeunique::Bool=false)
    rename!(index(ds), Symbol.(vals), makeunique=makeunique)
    _modified(_attributes(ds))
    return ds
end

function rename!(ds::AbstractDataset, args::AbstractVector{Pair{Symbol, Symbol}})
    rename!(index(ds), args)
    _modified(_attributes(ds))
    return ds
end

function rename!(ds::AbstractDataset,
                 args::Union{AbstractVector{<:Pair{Symbol, <:AbstractString}},
                             AbstractVector{<:Pair{<:AbstractString, Symbol}},
                             AbstractVector{<:Pair{<:AbstractString, <:AbstractString}},
                             AbstractDict{Symbol, Symbol},
                             AbstractDict{Symbol, <:AbstractString},
                             AbstractDict{<:AbstractString, Symbol},
                             AbstractDict{<:AbstractString, <:AbstractString}})
    rename!(index(ds), [Symbol(from) => Symbol(to) for (from, to) in args])
    _modified(_attributes(ds))
    return ds
end

function rename!(ds::AbstractDataset,
                 args::Union{AbstractVector{<:Pair{<:Integer, <:AbstractString}},
                             AbstractVector{<:Pair{<:Integer, Symbol}},
                             AbstractDict{<:Integer, <:AbstractString},
                             AbstractDict{<:Integer, Symbol}})
    rename!(index(ds), [_names(ds)[from] => Symbol(to) for (from, to) in args])
    _modified(_attributes(ds))
    return ds
end

function rename!(ds::AbstractDataset, args::Pair...)
    rename!(ds, collect(args))
    _modified(_attributes(ds))
    ds
end


function rename!(f::Function, ds::AbstractDataset)
    rename!(f, index(ds))
    _modified(_attributes(ds))
    return ds
end

"""
    rename(df::AbstractDataset, vals::AbstractVector{Symbol};
           makeunique::Bool=false)
    rename(df::AbstractDataset, vals::AbstractVector{<:AbstractString};
           makeunique::Bool=false)
    rename(df::AbstractDataset, (from => to)::Pair...)
    rename(df::AbstractDataset, d::AbstractDict)
    rename(df::AbstractDataset, d::AbstractVector{<:Pair})
    rename(f::Function, df::AbstractDataset)

Create a new data set that is a copy of `df` with changed column names.
Each name is changed at most once. Permutation of names is allowed.

# Arguments
- `df` : the `AbstractDataset`; if it is a `SubDataset` then renaming is
  only allowed if it was created using `:` as a column selector.
- `d` : an `AbstractDict` or an `AbstractVector` of `Pair`s that maps
  the original names or column numbers to new names
- `f` : a function which for each column takes the old name as a `String`
  and returns the new name that gets converted to a `Symbol`
- `vals` : new column names as a vector of `Symbol`s or `AbstractString`s
  of the same length as the number of columns in `df`
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

If pairs are passed to `rename` (as positional arguments or in a dictionary or
a vector) then:
* `from` value can be a `Symbol`, an `AbstractString` or an `Integer`;
* `to` value can be a `Symbol` or an `AbstractString`.

Mixing symbols and strings in `to` and `from` is not allowed.

See also: [`rename!`](@ref)

# Examples
```jldoctest
julia> df = Dataset(i = 1, x = 2, y = 3)
1×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, :i => :A, :x => :X)
1×3 Dataset
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, :x => :y, :y => :x)
1×3 Dataset
 Row │ i      y      x
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, [1 => :A, 2 => :X])
1×3 Dataset
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, Dict("i" => "A", "x" => "X"))
1×3 Dataset
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(uppercase, df)
1×3 Dataset
 Row │ I      X      Y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3
```
"""
rename(ds::AbstractDataset, vals::AbstractVector{Symbol};
       makeunique::Bool=false) = rename!(copy(ds), vals, makeunique=makeunique)
rename(ds::AbstractDataset, vals::AbstractVector{<:AbstractString};
       makeunique::Bool=false) = rename!(copy(ds), vals, makeunique=makeunique)
rename(ds::AbstractDataset, args...) = rename!(copy(ds), args...)
rename(f::Function, ds::AbstractDataset) = rename!(f, copy(ds))

"""
    size(df::AbstractDataset[, dim])

Return a tuple containing the number of rows and columns of `df`.
Optionally a dimension `dim` can be specified, where `1` corresponds to rows
and `2` corresponds to columns.

See also: [`nrow`](@ref), [`ncol`](@ref)

# Examples
```jldoctest
julia> df = Dataset(a=1:3, b='a':'c');

julia> size(df)
(3, 2)

julia> size(df, 1)
3
```
"""
Base.size(ds::AbstractDataset) = (nrow(ds), ncol(ds))
function Base.size(ds::AbstractDataset, i::Integer)
    if i == 1
        nrow(ds)
    elseif i == 2
        ncol(ds)
    else
        throw(ArgumentError("Datasets only have two dimensions"))
    end
end

Base.isempty(ds::AbstractDataset) = size(ds, 1) == 0 || size(ds, 2) == 0

if VERSION < v"1.6"
    Base.firstindex(ds::AbstractDataset, i::Integer) = first(axes(ds, i))
    Base.lastindex(ds::AbstractDataset, i::Integer) = last(axes(ds, i))
end
Base.axes(ds::AbstractDataset, i::Integer) = Base.OneTo(size(ds, i))

"""
    ndims(::AbstractDataset)
    ndims(::Type{<:AbstractDataset})

Return the number of dimensions of a data set, which is always `2`.
"""
Base.ndims(::AbstractDataset) = 2
Base.ndims(::Type{<:AbstractDataset}) = 2

# separate methods are needed due to dispatch ambiguity
Base.getproperty(ds::AbstractDataset, col_ind::Symbol) = ds[!, col_ind]
Base.getproperty(ds::AbstractDataset, col_ind::AbstractString) = ds[!, col_ind]

# Private fields are never exposed since they can conflict with column names
"""
    propertynames(ds::AbstractDataset)

Return a freshly allocated `Vector{Symbol}` of names of columns contained in `ds`.
"""
Base.propertynames(ds::AbstractDataset, private::Bool=false) = copy(_names(ds))

##############################################################################
##
## Similar
##
##############################################################################

"""
    similar(ds::AbstractDataset, rows::Integer=nrow(ds))

Create a new `Dataset` with the same column names and column element types
as `ds`. An optional second argument can be provided to request a number of rows
that is different than the number of rows present in `ds`.
"""
function Base.similar(ds::AbstractDataset, rows::Integer = size(ds, 1))
    rows < 0 && throw(ArgumentError("the number of rows must be non-negative"))
    # Create Dataset
    newds = Dataset(AbstractVector[similar(x, rows) for x in eachcol(ds)], copy(index(ds)),
              copycols=false)
    setinfo!(newds, _attributes(ds).meta.info[])
    newds
end

"""
    empty(ds::AbstractDataset)

Create a new `Dataset` with the same column names and column element types
as `ds` but with zero rows.
"""
Base.empty(ds::AbstractDataset) = similar(ds, 0)

##############################################################################
##
## Equality
##
##############################################################################

function Base.:(==)(ds1::AbstractDataset, ds2::AbstractDataset)
    size(ds1, 2) == size(ds2, 2) || return false
    isequal(index(ds1), index(ds2)) || return false
    eq = true
    for idx in 1:size(ds1, 2)
        coleq = ds1[!, idx] == ds2[!, idx]
        # coleq could be missing
        isequal(coleq, false) && return false
        eq &= coleq
    end
    return eq
end

function Base.isequal(ds1::AbstractDataset, ds2::AbstractDataset)
    size(ds1, 2) == size(ds2, 2) || return false
    isequal(index(ds1), index(ds2)) || return false
    for idx in 1:size(ds1, 2)
        isequal(ds1[!, idx], ds2[!, idx]) || return false
    end
    return true
end

"""
    isapprox(ds1::AbstractDataset, ds2::AbstractDataset;
             rtol::Real=atol>0 ? 0 : √eps, atol::Real=0,
             nans::Bool=false, norm::Function=norm)

Inexact equality comparison. `ds1` and `ds2` must have the same size and column names.
Return  `true` if `isapprox` with given keyword arguments
applied to all pairs of columns stored in `ds1` and `ds2` returns `true`.
"""
function Base.isapprox(ds1::AbstractDataset, ds2::AbstractDataset;
                       atol::Real=0, rtol::Real=atol>0 ? 0 : √eps(),
                       nans::Bool=false, norm::Function=norm)
    if size(ds1) != size(ds2)
        throw(DimensionMismatch("dimensions must match: a has dims " *
                                "$(size(ds1)), b has dims $(size(ds2))"))
    end
    if !isequal(index(ds1), index(ds2))
        throw(ArgumentError("column names of passed data sets do not match"))
    end
    return all(isapprox.(eachcol(ds1), eachcol(ds2), atol=atol, rtol=rtol, nans=nans, norm=norm))
end
##############################################################################
##
## Description
##
##############################################################################

"""
    only(ds::AbstractDataset)

If `ds` has a single row return it as a `DatasetRow`; otherwise throw `ArgumentError`.
"""
function only(ds::AbstractDataset)
    nrow(ds) != 1 && throw(ArgumentError("data set must contain exactly 1 row"))
    return ds[1, :]
end

"""
    first(ds::AbstractDataset)

Get the first row of `ds` as a `DatasetRow`.
"""
Base.first(ds::AbstractDataset) = ds[1, :]

"""
    first(ds::AbstractDataset, n::Integer)

Get a data set with the `n` first rows of `ds`.
"""
Base.first(ds::AbstractDataset, n::Integer) = ds[1:min(n, nrow(ds)), :]

"""
    last(ds::AbstractDataset)

Get the last row of `ds` as a `DatasetRow`.
"""
Base.last(ds::AbstractDataset) = ds[nrow(df), :]

"""
    last(ds::AbstractDataset, n::Integer)

Get a data set with the `n` last rows of `ds`.
"""
Base.last(ds::AbstractDataset, n::Integer) = ds[max(1, nrow(ds)-n+1):nrow(ds), :]


"""
    describe(ds::AbstractDataset; cols=:)
    describe(ds::AbstractDataset, stats::Union{Symbol, Pair}...; cols=:)

Return descriptive statistics for a data set as a new `Dataset`
where each row represents a variable and each column a summary statistic.

# Arguments
- `ds` : the `AbstractDataset`
- `stats::Union{Symbol, Pair}...` : the summary statistics to report.
  Arguments can be:
    - A symbol from the list `:mean`, `:std`, `:min`, `:q25`,
      `:median`, `:q75`, `:max`, `:eltype`, `:nunique`, `:first`, `:last`, and
      `:nmissing`. The default statistics used are `:mean`, `:min`, `:median`,
      `:max`, `:nmissing`, and `:eltype`.
    - `:all` as the only `Symbol` argument to return all statistics.
    - A `function => name` pair where `name` is a `Symbol` or string. This will
      create a column of summary statistics with the provided name.
- `cols` : a keyword argument allowing to select only a subset or transformation
  of columns from `ds` to describe. Can be any column selector or transformation
  accepted by [`select`](@ref).

# Details
For `Real` columns, compute the mean, standard deviation, minimum, first
quantile, median, third quantile, and maximum. If a column does not derive from
`Real`, `describe` will attempt to calculate all statistics, using `nothing` as
a fall-back in the case of an error.

When `stats` contains `:nunique`, `describe` will report the
number of unique values in a column. If a column's base type derives from `Real`,
`:nunique` will return `nothing`s.

Missing values are filtered in the calculation of all statistics, however the
column `:nmissing` will report the number of missing values of that variable.

If custom functions are provided, they are called repeatedly with the vector
corresponding to each column as the only argument. For columns allowing for
missing values, the vector is wrapped in a call to `skipmissing`: custom
functions must therefore support such objects (and not only vectors), and cannot
access missing values.

# Examples
```jldoctest
julia> ds = Dataset(i=1:10, x=0.1:0.1:1.0, y='a':'j');

julia> describe(ds)
3×7 Dataset
 Row │ variable  mean    min  median  max  nmissing  eltype
     │ Symbol    Union…  Any  Union…  Any  Int64     DataType
─────┼────────────────────────────────────────────────────────
   1 │ i         5.5     1    5.5     10          0  Int64
   2 │ x         0.55    0.1  0.55    1.0         0  Float64
   3 │ y                 a            j           0  Char

julia> describe(ds, :min, :max)
3×3 Dataset
 Row │ variable  min  max
     │ Symbol    Any  Any
─────┼────────────────────
   1 │ i         1    10
   2 │ x         0.1  1.0
   3 │ y         a    j

julia> describe(ds, :min, sum => :sum)
3×3 Dataset
 Row │ variable  min  sum
     │ Symbol    Any  Union…
─────┼───────────────────────
   1 │ i         1    55
   2 │ x         0.1  5.5
   3 │ y         a

julia> describe(ds, :min, sum => :sum, cols=:x)
1×3 Dataset
 Row │ variable  min      sum
     │ Symbol    Float64  Float64
─────┼────────────────────────────
   1 │ x             0.1      5.5
```
"""
DataAPI.describe(ds::AbstractDataset,
                 stats::Union{Symbol, Pair{<:Base.Callable, <:SymbolOrString}}...;
                 cols=:) =
    _describe(select(ds, cols, copycols=false), Any[s for s in stats])

DataAPI.describe(ds::AbstractDataset; cols=:) =
    _describe(select(ds, cols, copycols=false),
              Any[:mean, :min, :median, :max, :nmissing, :eltype])

function _describe(ds::AbstractDataset, stats::AbstractVector)
    predefined_funs = Symbol[s for s in stats if s isa Symbol]

    allowed_fields = [:mean, :std, :min, :q25, :median, :q75,
                      :max, :nunique, :nmissing, :first, :last, :eltype]

    if predefined_funs == [:all]
        predefined_funs = allowed_fields
        i = findfirst(s -> s == :all, stats)
        splice!(stats, i, allowed_fields) # insert in the stats vector to get a good order
    elseif :all in predefined_funs
        throw(ArgumentError("`:all` must be the only `Symbol` argument."))
    elseif !issubset(predefined_funs, allowed_fields)
        not_allowed = join(setdiff(predefined_funs, allowed_fields), ", :")
        allowed_msg = "\nAllowed fields are: :" * join(allowed_fields, ", :")
        throw(ArgumentError(":$not_allowed not allowed." * allowed_msg))
    end

    custom_funs = Any[s[1] => Symbol(s[2]) for s in stats if s isa Pair]

    ordered_names = [s isa Symbol ? s : Symbol(last(s)) for s in stats]

    if !allunique(ordered_names)
        df_ord_names = Dataset(ordered_names = ordered_names)
        duplicate_names = unique(ordered_names[nonunique(df_ord_names)])
        throw(ArgumentError("Duplicate names not allowed. Duplicated value(s) are: " *
                            ":$(join(duplicate_names, ", "))"))
    end

    # Put the summary stats into the return data set
    data = Dataset()
    data.variable = propertynames(ds)

    # An array of Dicts for summary statistics
    col_stats_dicts = map(eachcol(ds)) do col
        if eltype(col) >: Missing
            t = skipmissing(col)
            d = get_stats(t, predefined_funs)
            get_stats!(d, t, custom_funs)
        else
            d = get_stats(col, predefined_funs)
            get_stats!(d, col, custom_funs)
        end

        if :nmissing in predefined_funs
            d[:nmissing] = count(ismissing, col)
        end

        if :first in predefined_funs
            d[:first] = isempty(col) ? nothing : first(col)
        end

        if :last in predefined_funs
            d[:last] = isempty(col) ? nothing : last(col)
        end

        if :eltype in predefined_funs
            d[:eltype] = eltype(col)
        end

        return d
    end

    for stat in ordered_names
        # for each statistic, loop through the columns array to find values
        # letting the comprehension choose the appropriate type
        data[!, stat] = [d[stat] for d in col_stats_dicts]
    end

    return data
end

# Compute summary statistics
# use a dict because we dont know which measures the user wants
# Outside of the `describe` function due to something with 0.7
function get_stats(@nospecialize(col::Union{AbstractVector, Base.SkipMissing}),
                   stats::AbstractVector{Symbol})
    d = Dict{Symbol, Any}()

    if :q25 in stats || :median in stats || :q75 in stats
        q = try quantile(col, [.25, .5, .75]) catch; (nothing, nothing, nothing) end
        d[:q25] = q[1]
        d[:median] = q[2]
        d[:q75] = q[3]
    end

    if :min in stats || :max in stats
        ex = try extrema(col) catch; (nothing, nothing) end
        d[:min] = ex[1]
        d[:max] = ex[2]
    end

    if :mean in stats || :std in stats
        m = try mean(col) catch end
        # we can add non-necessary things to d, because we choose what we need
        # in the main function
        d[:mean] = m

        if :std in stats
            d[:std] = try std(col, mean = m) catch end
        end
    end

    if :nunique in stats
        if eltype(col) <: Real
            d[:nunique] = nothing
        else
            d[:nunique] = try length(Set(col)) catch end
        end
    end

    return d
end

function get_stats!(d::Dict, @nospecialize(col::Union{AbstractVector, Base.SkipMissing}),
                    stats::Vector{Any})
    for stat in stats
        d[stat[2]] = try stat[1](col) catch end
    end
end


##############################################################################
##
## Miscellaneous
##
##############################################################################

"""
    completecases(ds::AbstractDataset, cols=:)

Return a Boolean vector with `true` entries indicating rows without missing values
(complete cases) in data set `ds`.

If `cols` is provided, only missing values in the corresponding columns areconsidered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

See also: [`dropmissing`](@ref) and [`dropmissing!`](@ref).
Use `findall(completecases(ds))` to get the indices of the rows.

# Examples

```jldoctest
julia> ds = Dataset(i = 1:5,
                            x = [missing, 4, missing, 2, 1],
                            y = [missing, missing, "c", "d", "e"])
5×3 Dataset
 Row │ i      x        y
     │ Int64  Int64?   String?
─────┼─────────────────────────
   1 │     1  missing  missing
   2 │     2        4  missing
   3 │     3  missing  c
   4 │     4        2  d
   5 │     5        1  e

julia> completecases(ds)
5-element BitVector:
 0
 0
 0
 1
 1

julia> completecases(ds, :x)
5-element BitVector:
 0
 1
 0
 1
 1

julia> completecases(ds, [:x, :y])
5-element BitVector:
 0
 0
 0
 1
 1
```
"""
# byrow(any, ...) or byrow(all, ...) can handle the job
# function completecases(ds::AbstractDataset, col::Colon=:)
#     if ncol(ds) == 0
#         throw(ArgumentError("Unable to compute complete cases of a " *
#                             "data set with no columns"))
#     end
#     res = trues(size(ds, 1))
#     aux = BitVector(undef, size(ds, 1))
#     for i in 1:size(ds, 2)
#         v = ds[!, i]
#         if Missing <: eltype(v)
#             # Disable fused broadcasting as it happens to be much slower
#             aux .= .!ismissing.(v)
#             res .&= aux
#         end
#     end
#     return res
# end
#
# function completecases(df::AbstractDataset, col::ColumnIndex)
#     v = df[!, col]
#     if Missing <: eltype(v)
#         res = BitVector(undef, size(df, 1))
#         res .= .!ismissing.(v)
#         return res
#     else
#         return trues(size(df, 1))
#     end
# end
#
# completecases(df::AbstractDataset, cols::MultiColumnIndex) =
#     completecases(df[!, cols])
#
"""
    dropmissing(df::AbstractDataset, cols=:; view::Bool=false, disallowmissing::Bool=!view)

Return a data set excluding rows with missing values in `df`.

If `cols` is provided, only missing values in the corresponding columns are considered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `view=false` a freshly allocated `Dataset` is returned.
If `view=true` then a `SubDataset` view into `df` is returned. In this case
`disallowmissing` must be `false`.

If `disallowmissing` is `true` (the default when `view` is `false`)
then columns specified in `cols` will be converted so as not to allow for missing
values using [`disallowmissing!`](@ref).

See also: [`completecases`](@ref) and [`dropmissing!`](@ref).

# Examples

```jldoctest
julia> df = Dataset(i = 1:5,
                      x = [missing, 4, missing, 2, 1],
                      y = [missing, missing, "c", "d", "e"])
5×3 Dataset
 Row │ i      x        y
     │ Int64  Int64?   String?
─────┼─────────────────────────
   1 │     1  missing  missing
   2 │     2        4  missing
   3 │     3  missing  c
   4 │     4        2  d
   5 │     5        1  e

julia> dropmissing(df)
2×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e

julia> dropmissing(df, disallowmissing=false)
2×3 Dataset
 Row │ i      x       y
     │ Int64  Int64?  String?
─────┼────────────────────────
   1 │     4       2  d
   2 │     5       1  e

julia> dropmissing(df, :x)
3×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String?
─────┼───────────────────────
   1 │     2      4  missing
   2 │     4      2  d
   3 │     5      1  e

julia> dropmissing(df, [:x, :y])
2×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e
```
"""
# @inline function dropmissing(df::AbstractDataset,
#                              cols::Union{ColumnIndex, MultiColumnIndex}=:;
#                              view::Bool=false, disallowmissing::Bool=!view)
#     rowidxs = completecases(df, cols)
#     if view
#         if disallowmissing
#             throw(ArgumentError("disallowmissing=true is incompatible with view=true"))
#         end
#         return Base.view(df, rowidxs, :)
#     else
#         newdf = df[rowidxs, :]
#         disallowmissing && disallowmissing!(newdf, cols)
#         return newdf
#     end
# end

"""
    dropmissing!(df::AbstractDataset, cols=:; disallowmissing::Bool=true)

Remove rows with missing values from data set `df` and return it.

If `cols` is provided, only missing values in the corresponding columns are considered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `disallowmissing` is `true` (the default) then the `cols` columns will
get converted using [`disallowmissing!`](@ref).

See also: [`dropmissing`](@ref) and [`completecases`](@ref).

```jldoctest
julia> df = Dataset(i = 1:5,
                      x = [missing, 4, missing, 2, 1],
                      y = [missing, missing, "c", "d", "e"])
5×3 Dataset
 Row │ i      x        y
     │ Int64  Int64?   String?
─────┼─────────────────────────
   1 │     1  missing  missing
   2 │     2        4  missing
   3 │     3  missing  c
   4 │     4        2  d
   5 │     5        1  e

julia> dropmissing!(copy(df))
2×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e

julia> dropmissing!(copy(df), disallowmissing=false)
2×3 Dataset
 Row │ i      x       y
     │ Int64  Int64?  String?
─────┼────────────────────────
   1 │     4       2  d
   2 │     5       1  e

julia> dropmissing!(copy(df), :x)
3×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String?
─────┼───────────────────────
   1 │     2      4  missing
   2 │     4      2  d
   3 │     5      1  e

julia> dropmissing!(df, [:x, :y])
2×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e
```
"""
# function dropmissing!(df::AbstractDataset,
#                       cols::Union{ColumnIndex, MultiColumnIndex}=:;
#                       disallowmissing::Bool=true)
#     inds = completecases(df, cols)
#     inds .= .!(inds)
#     delete!(df, inds)
#     disallowmissing && disallowmissing!(df, cols)
#     df
# end

"""
    filter(fun, df::AbstractDataset; view::Bool=false)
    filter(cols => fun, df::AbstractDataset; view::Bool=false)

Return a data set containing only rows from `df` for which `fun`
returns `true`.

If `cols` is not specified then the predicate `fun` is passed `DatasetRow`s.

If `cols` is specified then the predicate `fun` is passed elements of the
corresponding columns as separate positional arguments, unless `cols` is an
`AsTable` selector, in which case a `NamedTuple` of these arguments is passed.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR), and
column duplicates are allowed if a vector of `Symbol`s, strings, or integers is
passed.

If `view=false` a freshly allocated `Dataset` is returned.
If `view=true` then a `SubDataset` view into `df` is returned.

Passing `cols` leads to a more efficient execution of the operation for large data sets.

See also: [`filter!`](@ref)

# Examples
```jldoctest
julia> df = Dataset(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
4×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> filter(row -> row.x > 1, df)
2×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter(:x => x -> x > 1, df)
2×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter([:x, :y] => (x, y) -> x == 1 || y == "b", df)
3×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b

julia> filter(AsTable(:) => nt -> nt.x == 1 || nt.y == "b", df)
3×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b
```
"""
@inline function Base.filter(f, ds::AbstractDataset; view::Bool=false)
    rowidxs = _filter_helper(f, eachrow(ds))
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end

@inline function Base.filter((cols, f)::Pair, ds::AbstractDataset; view::Bool=false)
    int_cols = index(ds)[cols] # it will be AbstractVector{Int} or Int
    if length(int_cols) == 0
        rowidxs = [f() for _ in axes(ds, 1)]
    else
        rowidxs = _filter_helper(f, (ds[!, i] for i in int_cols)...)
    end
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end

# this method is needed to allow for passing duplicate columns
@inline function Base.filter((cols, f)::Pair{<:Union{AbstractVector{<:Integer},
                                                     AbstractVector{<:AbstractString},
                                                     AbstractVector{<:Symbol}}},
                             ds::AbstractDataset; view::Bool=false)
    if length(cols) == 0
        rowidxs = [f() for _ in axes(ds, 1)]
    else
        rowidxs = _filter_helper(f, (ds[!, i] for i in cols)...)
    end
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end

_filter_helper(f, cols...)::BitVector = ((x...) -> f(x...)::Bool).(cols...)

# @inline function Base.filter((cols, f)::Pair{AsTable}, df::AbstractDataset;
#                              view::Bool=false)
#     df_tmp = select(df, cols.cols, copycols=false)
#     if ncol(df_tmp) == 0
#         rowidxs = [f(NamedTuple()) for _ in axes(df, 1)]
#     else
#         rowidxs = _filter_helper_astable(f, Tables.namedtupleiterator(df_tmp))
#     end
#     return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
# end

_filter_helper_astable(f, nti::Tables.NamedTupleIterator)::BitVector = (x -> f(x)::Bool).(nti)

"""
    filter!(fun, ds::AbstractDataset)
    filter!(cols => fun, ds::AbstractDataset)

Remove rows from data set `ds` for which `fun` returns `false`.

If `cols` is not specified then the predicate `fun` is passed `DatasetRow`s.

If `cols` is specified then the predicate `fun` is passed elements of the
corresponding columns as separate positional arguments, unless `cols` is an
`AsTable` selector, in which case a `NamedTuple` of these arguments is passed.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR), and
column duplicates are allowed if a vector of `Symbol`s, strings, or integers is
passed.

Passing `cols` leads to a more efficient execution of the operation for large data sets.

See also: [`filter`](@ref)

# Examples
```jldoctest
julia> ds = Dataset(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
4×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> filter!(row -> row.x > 1, ds)
2×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter!(:x => x -> x == 3, ds)
1×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b

julia> ds = Dataset(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"]);

julia> filter!([:x, :y] => (x, y) -> x == 1 || y == "b", ds)
3×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b

julia> ds = Dataset(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"]);

julia> filter!(AsTable(:) => nt -> nt.x == 1 || nt.y == "b", ds)
3×2 Dataset
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b
```
"""
Base.filter!(f, ds::AbstractDataset) = delete!(ds, findall(!f, eachrow(ds)))
Base.filter!((col, f)::Pair{<:ColumnIndex}, ds::AbstractDataset) =
    _filter!_helper(ds, f, ds[!, col])
Base.filter!((cols, f)::Pair{<:AbstractVector{Symbol}}, ds::AbstractDataset) =
    filter!([index(ds)[col] for col in cols] => f, ds)
Base.filter!((cols, f)::Pair{<:AbstractVector{<:AbstractString}}, ds::AbstractDataset) =
    filter!([index(ds)[col] for col in cols] => f, ds)
Base.filter!((cols, f)::Pair, ds::AbstractDataset) =
    filter!(index(ds)[cols] => f, ds)
Base.filter!((cols, f)::Pair{<:AbstractVector{Int}}, ds::AbstractDataset) =
    _filter!_helper(ds, f, (ds[!, i] for i in cols)...)

function _filter!_helper(ds::AbstractDataset, f, cols...)
    if length(cols) == 0
        rowidxs = findall(x -> !f(), axes(ds, 1))
    else
        rowidxs = findall(((x...) -> !(f(x...)::Bool)).(cols...))
    end
    return delete!(ds, rowidxs)
end

# function Base.filter!((cols, f)::Pair{<:AsTable}, df::AbstractDataset)
#     dff = select(df, cols.cols, copycols=false)
#     if ncol(dff) == 0
#         return delete!(df, findall(x -> !f(NamedTuple()), axes(df, 1)))
#     else
#         return _filter!_helper_astable(df, Tables.namedtupleiterator(dff), f)
#     end
# end

_filter!_helper_astable(ds::AbstractDataset, nti::Tables.NamedTupleIterator, f) =
    delete!(ds, _findall((x -> !(f(x)::Bool)).(nti)))

function Base.Matrix(ds::AbstractDataset)
    T = reduce(promote_type, (eltype(v) for v in eachcol(ds)))
    return Matrix{T}(ds)
end

function Base.Matrix{T}(ds::AbstractDataset) where T
    n, p = size(ds)
    res = Matrix{T}(undef, n, p)
    idx = 1
    for (name, col) in pairs(eachcol(ds))
        try
            copyto!(res, idx, col)
        catch err
            if err isa MethodError && err.f == convert &&
               !(T >: Missing) && any(ismissing, col)
                throw(ArgumentError("cannot convert a Dataset containing missing " *
                                    "values to Matrix{$T} (found for column $name)"))
            else
                rethrow(err)
            end
        end
        idx += n
    end
    return res
end

Base.Array(ds::AbstractDataset) = Matrix(ds)
Base.Array{T}(ds::AbstractDataset) where {T} = Matrix{T}(ds)

"""
    nonunique(ds::AbstractDataset)
    nonunique(ds::AbstractDataset, cols)

Return a `Vector{Bool}` in which `true` entries indicate duplicate rows.
A row is a duplicate if there exists a prior row with all columns containing
equal values (according to `isequal`).

See also [`unique`](@ref) and [`unique!`](@ref).

# Arguments
- `ds` : `AbstractDataset`
- `cols` : a selector specifying the column(s) or their transformations to compare.
  Can be any column selector or transformation accepted by [`select`](@ref).

# Examples
```jldoctest
julia> ds = Dataset(i = 1:4, x = [1, 2, 1, 2])
4×2 Dataset
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> ds = vcat(ds, ds)
8×2 Dataset
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> nonunique(ds)
8-element Vector{Bool}:
 0
 0
 0
 0
 1
 1
 1
 1

julia> nonunique(ds, 2)
8-element Vector{Bool}:
 0
 0
 1
 1
 1
 1
 1
 1
```
"""
function nonunique(ds::AbstractDataset, cols::MultiColumnIndex = :)
    if ncol(ds) == 0
        throw(ArgumentError("finding duplicate rows in data set with no " *
                            "columns is not allowed"))
    end

    # TODO is finding the first values of eachgroup easier????
    groups, gslots, ngroups = _create_dictionary(ds, cols, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
    res = trues(nrow(ds))
    seen_groups = falses(ngroups)
    # unique rows are the first encountered group representatives,
    # nonunique are everything else
    # @inbounds for g_row in gslots
    #     (g_row > 0) && (res[g_row] = false)
    # end
    @inbounds for i in 1:length(res)
        seen_groups[groups[i]] ? nothing : (seen_groups[groups[i]] = true; res[i] = false)
    end
    return res
end
nonunique(ds::AbstractDataset, col::ColumnIndex) = nonunique(ds, [col])

# nonunique(df::AbstractDataset, cols) = nonunique(select(df, cols, copycols=false))

# Modify Dataset
Base.unique!(ds::AbstractDataset) = delete!(ds, _findall(nonunique(ds)))
Base.unique!(ds::AbstractDataset, cols::AbstractVector) =
    delete!(ds, _findall(nonunique(ds, cols)))
Base.unique!(ds::AbstractDataset, cols) =
    delete!(ds, _findall(nonunique(ds, cols)))

# Unique rows of an AbstractDataset.
@inline function Base.unique(ds::AbstractDataset; view::Bool=false)
    rowidxs = (!).(nonunique(ds))
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end

@inline function Base.unique(ds::AbstractDataset, cols; view::Bool=false)
    rowidxs = (!).(nonunique(ds, cols))
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end

"""
    unique(ds::AbstractDataset; view::Bool=false)
    unique(ds::AbstractDataset, cols; view::Bool=false)
    unique!(ds::AbstractDataset)
    unique!(ds::AbstractDataset, cols)

Return a data set containing only the first occurrence of unique rows in `ds`.
When `cols` is specified, the returned `Dataset` contains complete rows,
retaining in each case the first occurrence of a given combination of values
in selected columns or their transformations. `cols` can be any column
selector or transformation accepted by [`select`](@ref).


For `unique`, if `view=false` a freshly allocated `Dataset` is returned,
and if `view=true` then a `SubDataset` view into `ds` is returned.

`unique!` updates `ds` in-place and does not support the `view` keyword argument.

See also [`nonunique`](@ref).

# Arguments
- `ds` : the AbstractDataset
- `cols` :  column indicator (Symbol, Int, Vector{Symbol}, Regex, etc.)
specifying the column(s) to compare.

# Examples
```jldoctest
julia> ds = Dataset(i = 1:4, x = [1, 2, 1, 2])
4×2 Dataset
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> ds = vcat(ds, ds)
8×2 Dataset
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> unique(ds)   # doesn't modify ds
4×2 Dataset
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> unique(ds, 2)
2×2 Dataset
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2

julia> unique!(ds)  # modifies ds
4×2 Dataset
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
```
"""
(unique, unique!)

"""
    hcat(ds::AbstractDataset...;
         makeunique::Bool=false, copycols::Bool=true)
    hcat(ds::AbstractDataset..., vs::AbstractVector;
         makeunique::Bool=false, copycols::Bool=true)
    hcat(vs::AbstractVector, ds::AbstractDataset;
         makeunique::Bool=false, copycols::Bool=true)

Horizontally concatenate `AbstractDatasets` and optionally `AbstractVector`s.

If `AbstractVector` is passed then a column name for it is automatically generated
as `:x1` by default.

If `makeunique=false` (the default) column names of passed objects must be unique.
If `makeunique=true` then duplicate column names will be suffixed
with `_i` (`i` starting at 1 for the first duplicate).

If `copycols=true` (the default) then the `Dataset` returned by `hcat` will
contain copied columns from the source data sets.
If `copycols=false` then it will contain columns as they are stored in the
source (without copying). This option should be used with caution as mutating
either the columns in sources or in the returned `Dataset` might lead to
the corruption of the other object.

# Example
```jldoctest
julia> ds1 = Dataset(A=1:3, B=1:3)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> ds2 = Dataset(A=4:6, B=4:6)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6

julia> ds3 = hcat(ds1, ds2, makeunique=true)
3×4 Dataset
 Row │ A      B      A_1    B_1
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      1      4      4
   2 │     2      2      5      5
   3 │     3      3      6      6

julia> ds3.A === ds1.A
false

julia> ds3 = hcat(ds1, ds2, makeunique=true, copycols=false);

julia> ds3.A === ds1.A
true
```
"""
Base.hcat(ds::AbstractDataset; makeunique::Bool=false, copycols::Bool=true) =
    Dataset(ds, copycols=copycols)
Base.hcat(ds::AbstractDataset, x; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(Dataset(ds, copycols=copycols), x,
          makeunique=makeunique, copycols=copycols)
Base.hcat(x, ds::AbstractDataset; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(x, ds, makeunique=makeunique, copycols=copycols)
Base.hcat(ds1::AbstractDataset, ds2::AbstractDataset;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(Dataset(ds1, copycols=copycols), ds2,
          makeunique=makeunique, copycols=copycols)
Base.hcat(ds::AbstractDataset, x, y...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(ds, x, makeunique=makeunique, copycols=copycols), y...,
          makeunique=makeunique, copycols=copycols)
Base.hcat(ds1::AbstractDataset, ds2::AbstractDataset, dsn::AbstractDataset...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(ds1, ds2, makeunique=makeunique, copycols=copycols), dsn...,
          makeunique=makeunique, copycols=copycols)

"""
    vcat(dss::AbstractDataset...;
         cols::Union{Symbol, AbstractVector{Symbol},
                     AbstractVector{<:AbstractString}}=:setequal,
         source::Union{Nothing, Symbol, AbstractString,
                       Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing)

Vertically concatenate `AbstractDataset`s.

The `cols` keyword argument determines the columns of the returned data set:

* `:setequal`: require all data sets to have the same column names disregarding
  order. If they appear in different orders, the order of the first provided data
  set is used.
* `:orderequal`: require all data sets to have the same column names and in the
  same order.
* `:intersect`: only the columns present in *all* provided data sets are kept.
  If the intersection is empty, an empty data set is returned.
* `:union`: columns present in *at least one* of the provided data sets are kept.
  Columns not present in some data sets are filled with `missing` where necessary.
* A vector of `Symbol`s or strings: only listed columns are kept.
  Columns not present in some data sets are filled with `missing` where necessary.

The `source` keyword argument, if not `nothing` (the default), specifies the
additional column to be added in the last position in the resulting data set
that will identify the source data set. It can be a `Symbol` or an
`AbstractString`, in which case the identifier will be the number of the passed
source data set, or a `Pair` consisting of a `Symbol` or an `AbstractString`
and of a vector specifying the data set identifiers (which do not have to be
unique). The name of the source column is not allowed to be present in any
source data set.

The order of columns is determined by the order they appear in the included data
sets, searching through the header of the first data set, then the second,
etc.

The element types of columns are determined using `promote_type`,
as with `vcat` for `AbstractVector`s.

`vcat` ignores empty data sets, making it possible to initialize an empty
data set at the beginning of a loop and `vcat` onto it.

# Example
```jldoctest
julia> ds1 = Dataset(A=1:3, B=1:3)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> ds2 = Dataset(A=4:6, B=4:6)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6

julia> ds3 = Dataset(A=7:9, C=7:9)
3×2 Dataset
 Row │ A      C
     │ Int64  Int64
─────┼──────────────
   1 │     7      7
   2 │     8      8
   3 │     9      9

julia> ds4 = Dataset()
0×0 Dataset

julia> vcat(ds1, ds2)
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

julia> vcat(ds1, ds3, cols=:union)
6×3 Dataset
 Row │ A      B        C
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │     1        1  missing
   2 │     2        2  missing
   3 │     3        3  missing
   4 │     7  missing        7
   5 │     8  missing        8
   6 │     9  missing        9

julia> vcat(ds1, ds3, cols=:intersect)
6×1 Dataset
 Row │ A
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3
   4 │     7
   5 │     8
   6 │     9

julia> vcat(ds4, ds1)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> vcat(ds1, ds2, ds3, ds4, cols=:union, source="source")
9×4 Dataset
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Int64
─────┼─────────────────────────────────
   1 │     1        1  missing       1
   2 │     2        2  missing       1
   3 │     3        3  missing       1
   4 │     4        4  missing       2
   5 │     5        5  missing       2
   6 │     6        6  missing       2
   7 │     7  missing        7       3
   8 │     8  missing        8       3
   9 │     9  missing        9       3

julia> vcat(ds1, ds2, ds4, ds3, cols=:union, source=:source => 'a':'d')
9×4 Dataset
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Char
─────┼─────────────────────────────────
   1 │     1        1  missing  a
   2 │     2        2  missing  a
   3 │     3        3  missing  a
   4 │     4        4  missing  b
   5 │     5        5  missing  b
   6 │     6        6  missing  b
   7 │     7  missing        7  d
   8 │     8  missing        8  d
   9 │     9  missing        9  d
```
"""
Base.vcat(dss::AbstractDataset...;
          cols::Union{Symbol, AbstractVector{Symbol},
                      AbstractVector{<:AbstractString}}=:setequal,
          source::Union{Nothing, SymbolOrString,
                           Pair{<:SymbolOrString, <:AbstractVector}}=nothing) =
    reduce(vcat, dss; cols=cols, source=source)

"""
    reduce(::typeof(vcat),
           dss::Union{AbstractVector{<:AbstractDataset},
                      Tuple{AbstractDataset, Vararg{AbstractDataset}}};
           cols::Union{Symbol, AbstractVector{Symbol},
                       AbstractVector{<:AbstractString}}=:setequal,
           source::Union{Nothing, Symbol, AbstractString,
                         Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing)

Efficiently reduce the given vector or tuple of `AbstractDataset`s with `vcat`.

The column order, names, and types of the resulting `Dataset`, and
the behavior of `cols` and `source` keyword arguments follow the rules specified
for [`vcat`](@ref) of `AbstractDataset`s.

# Example
```jldoctest
julia> ds1 = Dataset(A=1:3, B=1:3)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> ds2 = Dataset(A=4:6, B=4:6)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6

julia> ds3 = Dataset(A=7:9, C=7:9)
3×2 Dataset
 Row │ A      C
     │ Int64  Int64
─────┼──────────────
   1 │     7      7
   2 │     8      8
   3 │     9      9

julia> reduce(vcat, (ds1, ds2))
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

julia> reduce(vcat, [ds1, ds2, ds3], cols=:union, source=:source)
9×4 Dataset
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Int64
─────┼─────────────────────────────────
   1 │     1        1  missing       1
   2 │     2        2  missing       1
   3 │     3        3  missing       1
   4 │     4        4  missing       2
   5 │     5        5  missing       2
   6 │     6        6  missing       2
   7 │     7  missing        7       3
   8 │     8  missing        8       3
   9 │     9  missing        9       3
```
"""
function Base.reduce(::typeof(vcat),
                     dss::Union{AbstractVector{<:AbstractDataset},
                                Tuple{AbstractDataset, Vararg{AbstractDataset}}};
                     cols::Union{Symbol, AbstractVector{Symbol},
                                 AbstractVector{<:AbstractString}}=:setequal,
                     source::Union{Nothing, SymbolOrString,
                                   Pair{<:SymbolOrString, <:AbstractVector}}=nothing)
    # Create Dataset
    res = _vcat(AbstractDataset[ds for ds in dss if ncol(ds) != 0]; cols=cols)
    if source !== nothing
        len = length(dss)
        if source isa SymbolOrString
            col, vals = source, 1:len
        else
            @assert source isa Pair{<:SymbolOrString, <:AbstractVector}
            col, vals = source
        end

        if columnindex(res, col) > 0
            idx = findfirst(ds -> columnindex(ds, col) > 0, dss)
            @assert idx !== nothing
            throw(ArgumentError("source column name :$col already exists in data set " *
                                " passed in position $idx"))
        end

        if len != length(vals)
            throw(ArgumentError("number of passed source identifiers ($(length(vals)))" *
                                "does not match the number of data sets ($len)"))
        end

        source_vec = Tables.allocatecolumn(eltype(vals), nrow(res))
        @assert firstindex(source_vec) == 1 && lastindex(source_vec) == nrow(res)
        start = 1
        for (v, ds) in zip(vals, dss)
            stop = start + nrow(ds) - 1
            source_vec[start:stop] .= Ref(v)
            start = stop + 1
        end

        @assert start == nrow(res) + 1
        insertcols!(res, col => source_vec)
    end

    return res
end

# Create Dataset
# TODO how formats are going to be transferred???
function _vcat(dss::AbstractVector{AbstractDataset};
               cols::Union{Symbol, AbstractVector{Symbol},
                           AbstractVector{<:AbstractString}}=:setequal)

    isempty(dss) && return Dataset()
    # Array of all headers
    allheaders = map(names, dss)
    # Array of unique headers across all data sets
    uniqueheaders = unique(allheaders)
    # All symbols present across all headers
    unionunique = union(uniqueheaders...)
    # List of symbols present in all datasets
    intersectunique = intersect(uniqueheaders...)

    if cols === :orderequal
        header = unionunique
        if length(uniqueheaders) > 1
            throw(ArgumentError("when `cols=:orderequal` all data sets need to " *
                                "have the same column names and be in the same order"))
        end
    elseif cols === :setequal || cols === :equal
        # an explicit error is thrown as :equal was supported in the past
        if cols === :equal
            throw(ArgumentError("`cols=:equal` is not supported. " *
                                "Use `:setequal` instead."))
        end

        header = unionunique
        coldiff = setdiff(unionunique, intersectunique)

        if !isempty(coldiff)
            # if any Datasets are a full superset of names, skip them
            let header=header     # julia #15276
                filter!(u -> !issetequal(u, header), uniqueheaders)
            end
            estrings = map(enumerate(uniqueheaders)) do (i, head)
                matching = findall(h -> head == h, allheaders)
                headerdiff = setdiff(coldiff, head)
                badcols = join(headerdiff, ", ", " and ")
                args = join(matching, ", ", " and ")
                return "column(s) $badcols are missing from argument(s) $args"
            end
            throw(ArgumentError(join(estrings, ", ", ", and ")))
        end
    elseif cols === :intersect
        header = intersectunique
    elseif cols === :union
        header = unionunique
    elseif cols isa Symbol
        throw(ArgumentError("Invalid `cols` value :$cols. " *
                            "Only `:orderequal`, `:setequal`, `:intersect`, " *
                            "`:union`, or a vector of column names is allowed."))
    elseif cols isa AbstractVector{Symbol}
        header = cols
    else
        @assert cols isa AbstractVector{<:AbstractString}
        header = Symbol.(cols)
    end

    length(header) == 0 && return Dataset()
    all_cols = Vector{AbstractVector}(undef, length(header))
    for (i, name) in enumerate(header)
        newcols = map(dss) do ds
            if hasproperty(ds, name)
                return ds[!, name]
            else
                Iterators.repeated(missing, nrow(ds))
            end
        end

        lens = map(length, newcols)
        T = mapreduce(eltype, promote_type, newcols)
        all_cols[i] = Tables.allocatecolumn(T, sum(lens))
        offset = 1
        for j in 1:length(newcols)
            copyto!(all_cols[i], offset, newcols[j])
            offset += lens[j]
        end
    end
    return Dataset(all_cols, header, copycols=false)
end

"""
    repeat(ds::AbstractDataset; inner::Integer = 1, outer::Integer = 1)

Construct a data set by repeating rows in `ds`. `inner` specifies how many
times each row is repeated, and `outer` specifies how many times the full set
of rows is repeated.

# Example
```jldoctest
julia> ds = Dataset(a = 1:2, b = 3:4)
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> repeat(ds, inner = 2, outer = 3)
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
function Base.repeat(ds::AbstractDataset; inner::Integer=1, outer::Integer=1)
    inner < 0 && throw(ArgumentError("inner keyword argument must be non-negative"))
    outer < 0 && throw(ArgumentError("outer keyword argument must be non-negative"))
    return mapcols(x -> repeat(x, inner = Int(inner), outer = Int(outer)), ds)
end

"""
    repeat(ds::AbstractDataset, count::Integer)

Construct a data set by repeating each row in `ds` the number of times
specified by `count`.

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
function Base.repeat(ds::AbstractDataset, count::Integer)
    count < 0 && throw(ArgumentError("count must be non-negative"))
    return mapcols(x -> repeat(x, Int(count)), ds)
end

##############################################################################
##
## Hashing
##
##############################################################################

const hashds_seed = UInt == UInt32 ? 0xfd8bb02e : 0x6215bada8c8c46de

function Base.hash(ds::AbstractDataset, h::UInt)
    h += hashds_seed
    h += hash(size(ds))
    for i in 1:size(ds, 2)
        h = hash(ds[!, i], h)
    end
    return h
end

Base.parent(ads::AbstractDataset) = ads
Base.parentindices(ads::AbstractDataset) = axes(ads)

## Documentation for methods defined elsewhere

function nrow end
function ncol end

"""
    nrow(ds::AbstractDataset)
    ncol(ds::AbstractDataset)

Return the number of rows or columns in an `AbstractDataset` `ds`.

See also [`size`](@ref).

**Examples**

```jldoctest
julia> ds = Dataset(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10));

julia> size(ds)
(10, 3)

julia> nrow(ds)
10

julia> ncol(ds)
3
```

"""
(nrow, ncol)

"""
    disallowmissing(ds::AbstractDataset, cols=:; error::Bool=true)

Return a copy of data set `ds` with columns `cols` converted
from element type `Union{T, Missing}` to `T` to drop support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data set are converted.

If `error=false` then columns containing a `missing` value will be skipped instead
of throwing an error.

**Examples**

```jldoctest
julia> ds = Dataset(a=Union{Int, Missing}[1, 2])
2×1 Dataset
 Row │ a
     │ Int64?
─────┼────────
   1 │      1
   2 │      2

julia> disallowmissing(ds)
2×1 Dataset
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> ds = Dataset(a=[1, missing])
2×1 Dataset
 Row │ a
     │ Int64?
─────┼─────────
   1 │       1
   2 │ missing

julia> disallowmissing(ds, error=false)
2×1 Dataset
 Row │ a
     │ Int64?
─────┼─────────
   1 │       1
   2 │ missing
```
"""
function Missings.disallowmissing(ds::AbstractDataset,
                                  cols::Union{ColumnIndex, MultiColumnIndex}=:;
                                  error::Bool=true)
    # Create Dataset
    idxcols = Set(index(ds)[cols])
    newcols = AbstractVector[]
    for i in axes(ds, 2)
        x = ds[!, i]
        if i in idxcols
            if !error && Missing <: eltype(x) && any(ismissing, x)
                y = x
            else
                y = disallowmissing(x)
            end
            push!(newcols, y === x ? copy(y) : y)
        else
            push!(newcols, copy(x))
        end
    end
    newds = Dataset(newcols, _names(ds), copycols=false)
    setformat!(newds, _getformats(ds))
    setinfo!(newds, _attributes(ds).meta.info[])
    newds
end

"""
    allowmissing(ds::AbstractDataset, cols=:)

Return a copy of data set `ds` with columns `cols` converted
to element type `Union{T, Missing}` from `T` to allow support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data set are converted.

**Examples**

```jldoctest
julia> ds = Dataset(a=[1, 2])
2×1 Dataset
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> allowmissing(ds)
2×1 Dataset
 Row │ a
     │ Int64?
─────┼────────
   1 │      1
   2 │      2
```
"""
function Missings.allowmissing(ds::AbstractDataset,
                               cols::Union{ColumnIndex, MultiColumnIndex}=:)
    # Create Dataset
    idxcols = Set(index(ds)[cols])
    newcols = AbstractVector[]
    for i in axes(ds, 2)
        x = ds[!, i]
        if i in idxcols
            y = allowmissing(x)
            push!(newcols, y === x ? copy(y) : y)
        else
            push!(newcols, copy(x))
        end
    end
    newds = Dataset(newcols, _names(ds), copycols=false)
    setformat!(newds, _getformats(ds))
    setinfo!(newds, _attributes(ds).meta.info[])
    newds
end

"""
    flatten(ds::AbstractDataset, cols)

When columns `cols` of data set `ds` have iterable elements that define
`length` (for example a `Vector` of `Vector`s), return a `Dataset` where each
element of each `col` in `cols` is flattened, meaning the column corresponding
to `col` becomes a longer vector where the original entries are concatenated.
Elements of row `i` of `ds` in columns other than `cols` will be repeated
according to the length of `ds[i, col]`. These lengths must therefore be the
same for each `col` in `cols`, or else an error is raised. Note that these
elements are not copied, and thus if they are mutable changing them in the
returned `Dataset` will affect `ds`.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

# Examples

```jldoctest
julia> ds1 = Dataset(a = [1, 2], b = [[1, 2], [3, 4]], c = [[5, 6], [7, 8]])
2×3 Dataset
 Row │ a      b       c
     │ Int64  Array…  Array…
─────┼───────────────────────
   1 │     1  [1, 2]  [5, 6]
   2 │     2  [3, 4]  [7, 8]

julia> flatten(ds1, :b)
4×3 Dataset
 Row │ a      b      c
     │ Int64  Int64  Array…
─────┼──────────────────────
   1 │     1      1  [5, 6]
   2 │     1      2  [5, 6]
   3 │     2      3  [7, 8]
   4 │     2      4  [7, 8]

julia> flatten(ds1, [:b, :c])
4×3 Dataset
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      5
   2 │     1      2      6
   3 │     2      3      7
   4 │     2      4      8

julia> ds2 = Dataset(a = [1, 2], b = [("p", "q"), ("r", "s")])
2×2 Dataset
 Row │ a      b
     │ Int64  Tuple…
─────┼───────────────────
   1 │     1  ("p", "q")
   2 │     2  ("r", "s")

julia> flatten(ds2, :b)
4×2 Dataset
 Row │ a      b
     │ Int64  String
─────┼───────────────
   1 │     1  p
   2 │     1  q
   3 │     2  r
   4 │     2  s

julia> ds3 = Dataset(a = [1, 2], b = [[1, 2], [3, 4]], c = [[5, 6], [7]])
2×3 Dataset
 Row │ a      b       c
     │ Int64  Array…  Array…
─────┼───────────────────────
   1 │     1  [1, 2]  [5, 6]
   2 │     2  [3, 4]  [7]

julia> flatten(ds3, [:b, :c])
ERROR: ArgumentError: Lengths of iterables stored in columns :b and :c are not the same in row 2
```
"""
function flatten(ds::AbstractDataset,
                 cols::Union{ColumnIndex, MultiColumnIndex})
    # Create Dataset
    _check_consistency(ds)

    idxcols = index(ds)[cols]
    isempty(idxcols) && return copy(ds)
    col1 = first(idxcols)
    lengths = length.(ds[!, col1])
    for col in idxcols
        v = ds[!, col]
        if any(x -> length(x[1]) != x[2], zip(v, lengths))
            r = findfirst(x -> x != 0, length.(v) .- lengths)
            colnames = _names(ds)
            throw(ArgumentError("Lengths of iterables stored in columns :$(colnames[col1]) " *
                                "and :$(colnames[col]) are not the same in row $r"))
        end
    end

    new_ds = similar(ds[!, Not(cols)], sum(lengths))
    for name in _names(new_ds)
        repeat_lengths!(new_ds[!, name], ds[!, name], lengths)
    end
    length(idxcols) > 1 && sort!(idxcols)
    for col in idxcols
        col_to_flatten = ds[!, col]
        flattened_col = col_to_flatten isa AbstractVector{<:AbstractVector} ?
            reduce(vcat, col_to_flatten) :
            collect(Iterators.flatten(col_to_flatten))

        insertcols!(new_ds, col, _names(ds)[col] => flattened_col)
    end

    return new_ds
end

function repeat_lengths!(longnew::AbstractVector, shortold::AbstractVector,
                         lengths::AbstractVector{Int})
    counter = 1
    @inbounds for i in eachindex(shortold)
        l = lengths[i]
        longnew[counter:(counter + l - 1)] .= Ref(shortold[i])
        counter += l
    end
end

# Disallowed operations that are a common mistake

Base.getindex(::AbstractDataset, ::Union{Symbol, Integer, AbstractString}) =
    throw(ArgumentError("syntax ds[column] is not supported use ds[!, column] instead"))

Base.setindex!(::AbstractDataset, ::Any, ::Union{Symbol, Integer, AbstractString}) =
    throw(ArgumentError("syntax ds[column] is not supported use ds[!, column] instead"))
