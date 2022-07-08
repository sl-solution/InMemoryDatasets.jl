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
#
# # Modify Dataset
function allowmissing!(ds::Dataset, col::ColumnIndex)
    colidx = index(ds)[col]
    # use _columns to avoid reseting format or grouping info
    _columns(ds)[colidx] = allowmissing(_columns(ds)[colidx])
    _modified(_attributes(ds))
    return ds
end
#
# # Modify Dataset
function allowmissing!(ds::Dataset, cols::AbstractVector{<:ColumnIndex})
    for col in cols
        allowmissing!(ds, col)
    end
    return ds
end
#
# # Modify Dataset
function allowmissing!(ds::Dataset, cols::AbstractVector{Bool})
    length(cols) == size(ds, 2) || throw(BoundsError(ds, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && allowmissing!(ds, col)
    end
    return ds
end
#
# # Modify Dataset
allowmissing!(ds::Dataset, cols::MultiColumnIndex) =
    allowmissing!(ds, index(ds)[cols])

# Modify Dataset
allowmissing!(ds::Dataset, cols::Colon=:) =
    allowmissing!(ds, axes(ds, 2))

"""
    disallowmissing!(ds::Dataset, cols=:; error::Bool=false)

Convert columns `cols` of data set `ds` from element type `Union{T, Missing}` to
`T` to drop support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data set are converted.

If `error=false` then columns containing a `missing` value will be skipped instead
of throwing an error.
"""
function disallowmissing! end

# Modify Dataset
function disallowmissing!(ds::Dataset, col::ColumnIndex; error::Bool=false)
    x = _columns(ds)[col]
    colidx = index(ds)[col]
    if !(!error && Missing <: eltype(x) && any(ismissing, x))
        # use _columns to avoid reseting attributes
         _columns(ds)[colidx] = disallowmissing(x)
    end
    _modified(_attributes(ds))
    return ds
end
#
# # Modify Dataset
function disallowmissing!(ds::Dataset, cols::AbstractVector{<:ColumnIndex};
                          error::Bool=false)
    for col in cols
        disallowmissing!(ds, col, error=error)
    end
    return ds
end
#
# Modify Dataset
function disallowmissing!(ds::Dataset, cols::AbstractVector{Bool}; error::Bool=false)
    length(cols) == size(ds, 2) || throw(BoundsError(ds, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && disallowmissing!(ds, col, error=error)
    end
    return ds
end
#
# Modify Dataset
disallowmissing!(ds::Dataset, cols::MultiColumnIndex; error::Bool=false) =
    disallowmissing!(ds, index(ds)[cols], error=error)

# Modify Dataset
disallowmissing!(ds::Dataset, cols::Colon=:; error::Bool=false) =
    disallowmissing!(ds, axes(ds, 2), error=error)

"""
    repeat!(ds::Dataset; inner::Integer = 1, outer::Integer = 1, freq = nothing)
    repeat!(ds::Dataset, count) = repeat!(ds, outer = count)

Update a data set `ds` in-place by repeating its rows.

* `inner` specifies how many times each row is repeated,
* `outer` specifies how many times the full set of rows is repeated, and
* `freq` allow user to pass a vector of integers or a column name or index which indicate how many times the corresponding row should be repeated.
When `freq` is passed, the values must be positive integer values or zero (zero means the corresponding row should be dropped).


# Example
```jldoctest
julia> ds = Dataset(a = 1:2, b = 3:4)
2×2 Dataset
 Row │ a         b
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        2         4

julia> repeat!(ds, inner = 2, outer = 3);

julia> ds
12×2 Dataset
 Row │ a         b
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        1         3
   3 │        2         4
   4 │        2         4
   5 │        1         3
   6 │        1         3
   7 │        2         4
   8 │        2         4
   9 │        1         3
  10 │        1         3
  11 │        2         4
  12 │        2         4

julia> ds = Dataset(a = 1:2, b = 3:4)
2×2 Dataset
 Row │ a         b
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        2         4

julia> repeat!(ds, freq = :a)
3×2 Dataset
 Row │ a         b
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        2         4
   3 │        2         4
```
"""
function repeat!(ds::Dataset; inner::Integer = 1, outer::Integer = 1, freq::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex, Nothing} = nothing)
    T = nrow(ds) < typemax(Int8) ? Int8 : nrow(ds) < typemax(Int32) ? Int32 : Int64
# Modify Dataset
    if freq === nothing
        inner <= 0 && throw(ArgumentError("inner keyword argument must be greater than zero"))
        outer <= 0 && throw(ArgumentError("outer keyword argument must be greater than zero"))
        r_indx = repeat(T(1):T(nrow(ds)), inner = inner, outer = outer)
        _permute_ds_after_sort!(ds, r_indx, check = false)
        _reset_grouping_info!(ds)

        _modified(_attributes(ds))
        ds
    else
        if freq isa SubDatasetColumn || freq isa DatasetColumn
            lengths = __!(freq)
        elseif freq isa ColumnIndex
            lengths = _columns(ds)[index(ds)[freq]]
        elseif freq isa AbstractVector
            lengths = freq
        end
        if !(eltype(lengths) <: Union{Missing, Integer}) || any(ismissing, lengths) || any(x->isless(x, 0), lengths)
            throw(ArgumentError("The column selected for repeating must be an integer column with all values greater than or equal to zero and with no missing values"))
        end
        if length(lengths) != nrow(ds)
            throw(ArgumentError("The length of frequencies must match the number of rows in passed data set"))
        end
        r_index = _create_index_for_repeat(lengths, Val(T))
        _permute_ds_after_sort!(ds, r_index, check = false)
        _reset_grouping_info!(ds)
        _modified(_attributes(ds))
        ds
    end
end
function _fill_index_for_repeat!(res, w)
    counter = 1
    for i in 1:length(w)
        for j in 1:w[i]
            res[counter] = i
            counter += 1
        end
    end
end
# use this to create index for new data set
# and then use getindex with the result of this function for repeating
# This should be better for repeating large data set/ since getindex is threaded
function _create_index_for_repeat(w, ::Val{T}) where T
    res = Vector{T}(undef, sum(w))
    _fill_index_for_repeat!(res, w)
    res
end

function repeat!(ds::Dataset, count::Integer)

# Modify Dataset
    count <= 0 && throw(ArgumentError("count must be greater than zero"))
    repeat!(ds, inner = 1, outer = count)
    ds
end

"""
    repeat(ds::AbstractDataset; inner::Integer = 1, outer::Integer = 1, freq = nothing, view = false)
    repeat(ds::AbstractDataset, count) = repeat!(ds, outer = count, view = false)

Variant of `repeat!` which returns a fresh copy of passed data set. If `view = true` a view of the result will be returned.

# Example
```jldoctest
julia> ds = Dataset(a = 1:2, b = 3:4)
2×2 Dataset
 Row │ a         b
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        2         4

julia> repeat(ds, inner = 2, outer = 3)
12×2 Dataset
 Row │ a         b
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        1         3
   3 │        2         4
   4 │        2         4
   5 │        1         3
   6 │        1         3
   7 │        2         4
   8 │        2         4
   9 │        1         3
  10 │        1         3
  11 │        2         4
  12 │        2         4

julia> repeat(ds, freq = :a)
3×2 Dataset
 Row │ a         b
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        2         4
   3 │        2         4
```
"""
Base.repeat(ds::AbstractDataset, count::Integer; view = false) = repeat(ds, outer = count, view = view)
function Base.repeat(ds::AbstractDataset; inner::Integer = 1, outer::Integer = 1, freq = nothing, view = false)
    T = nrow(ds) < typemax(Int8) ? Int8 : nrow(ds) < typemax(Int32) ? Int32 : Int64

    if freq === nothing
        if view
            inner <= 0 && throw(ArgumentError("inner keyword argument must be greater than zero"))
            outer <= 0 && throw(ArgumentError("outer keyword argument must be greater than zero"))
            r_indx = repeat(T(1):T(nrow(ds)), inner = inner, outer = outer)
            Base.view(ds, r_indx, :)
        else
            repeat!(copy(ds), inner = inner, outer = outer)
        end
    else
        if view
            if freq isa SubDatasetColumn || freq isa DatasetColumn
                lengths = __!(freq)
            elseif freq isa ColumnIndex
                lengths = _columns(ds)[index(ds)[freq]]
            elseif freq isa AbstractVector
                lengths = freq
            end
            if !(eltype(lengths) <: Union{Missing, Integer}) || any(ismissing, lengths) || any(x->isless(x, 0), lengths)
                throw(ArgumentError("The column selected for repeating must be an integer column with all values greater than or equal to zero and with no missing values"))
            end
            if length(lengths) != nrow(ds)
                throw(ArgumentError("The length of frequencies must match the number of rows in passed data set"))
            end
            r_index = _create_index_for_repeat(lengths, Val(T))
            Base.view(ds, r_index, :)
        else
            repeat!(copy(ds), freq = freq)
        end
    end
end

# This is not exactly copy! as in general we allow axes to be different

# Modify Dataset
function _replace_columns!(ds::Dataset, newds::Dataset)
    copy!(_columns(ds), _columns(newds))
    copy!(_names(index(ds)), _names(newds))
    copy!(index(ds).lookup, index(newds).lookup)
    copy!(index(ds).format, index(newds).format)
    _copy_grouping_info!(ds, newds)
    _modified(_attributes(ds))
    _attributes(ds).meta.info[] = _attributes(newds).meta.info[]
    # created date cannot be modified
    return ds
end


"""
    map(ds::AbstractDataset, f::Function, cols; [threads = true])
    map(ds::AbstractDataset, f::Vector{Function}, cols; [threads = true])

Return a copy of `ds` where cols of the new `Dataset` is the result of calling `f` on each observation. The order of columns for the new data set is the same as `ds`.
Note that `map` guarantees not to reuse the columns from `ds` in the returned
`Dataset`. If `f` returns its argument then it gets copied before being stored.
The number of functions and the number of cols must match when multiple functions is used.

See [`map!`](@ref)

# Examples
```jldoctest
julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        11
   2 │        2        12
   3 │        3        13
   4 │        4        14

julia> map(ds, x -> x^2, :)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1       121
   2 │        4       144
   3 │        9       169
   4 │       16       196
```
"""
function Base.map(ds::AbstractDataset, f::Function, cols::MultiColumnIndex; threads = true)
    colsidx = index(ds)[cols]
    fs = repeat([f], length(colsidx))
    map(ds, fs, cols; threads = threads)
end
Base.map(ds::AbstractDataset, f::Function, col::ColumnIndex; threads = true) = map(ds, f, [col]; threads = threads)
Base.map(ds::AbstractDataset, f::Vector{Function}; threads = true) = throw(ArgumentError("the `cols` argument cannot be left blank"))


function Base.map(ds::AbstractDataset, f::Vector{<:Function}, cols::MultiColumnIndex; threads = true)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    @assert length(f) ==length(colsidx) "The number of functions and the number of columns must match"
    if ds isa SubDataset
        transfer_grouping_info = false
    else
        transfer_grouping_info = !any(colsidx .∈ Ref(index(ds).sortedcols))
    end
    sort_perm_colsidx = sortperm(colsidx)
    # sorted_colsidx = sort(colsidx)
    counter_f = 1
    vs = AbstractVector[]
    for j in 1:ncol(ds)
        if insorted(j, view(colsidx, sort_perm_colsidx))
            _f = f[sort_perm_colsidx[counter_f]]
            counter_f += 1
        else
            _f = identity
        end
        v = _columns(ds)[j]

        if threads
            T = Core.Compiler.return_type(_f, Tuple{eltype(v)})
            fv = _our_vect_alloc(T, length(v))
            _hp_map_a_function!(fv, _f, v)
        else
            fv = map(_f, v)
        end
        push!(vs, fv === v ? copy(fv) : fv)
    end
    if transfer_grouping_info
        newds_index = copy(index(ds))
    else
        newds_index = copy(index(ds))
        _reset_grouping_info!(newds_index)
    end
    # formats don't need to be transferred
    newds = Dataset(vs, newds_index, copycols=false)
    removeformat!(newds, cols)
    setinfo!(newds, _attributes(ds).meta.info[])
    return newds

end


"""
    map!(ds::AbstractDataset, f::Function, cols; [threads = true])
    map!(ds::AbstractDataset, f::Vector{Function}, cols; [threads = true])

Update each row of each `col` in `ds[!, cols]` in-place when `map!` return a result, and skip when it is not possible.

If `f` cannot be applied in place, use `map` for creating a copy of `ds`.

See [`map`](@ref)

# Examples

```jldoctest
julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        11
   2 │        2        12
   3 │        3        13
   4 │        4        14

julia> map!(ds, x -> x^2, :);

julia> ds
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1       121
   2 │        4       144
   3 │        9       169
   4 │       16       196

julia> ds = Dataset(x=1:4, y=11:14)
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        11
   2 │        2        12
   3 │        3        13
   4 │        4        14

julia> map!(ds, [x -> x^2, x -> x-1], :);

julia> ds
4×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1        10
   2 │        4        11
   3 │        9        12
   4 │       16        13
```
"""
function Base.map!(ds::AbstractDataset, f::Vector{<:Function}, cols::MultiColumnIndex; threads = true)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "The number of functions and the number of columns must match"
    _reset_group = false
    # TODO needs function barrier
    number_of_warnnings = 0
    for j in 1:length(colsidx)
        CT = eltype(_columns(ds)[colsidx[j]])
        # Core.Compiler.return_type cannot handle the situations like x->ismissing(x) ? 0 : x when x is missing and float, since the output of Core.Compiler.return_type is Union{Missing, Float64, Int64}
        # we remove missing and then check the result,
        # TODO is there any problem with this?
        T = Core.Compiler.return_type(f[j], Tuple{nonmissingtype(CT)})
        T = Union{Missing, T}
        if promote_type(T, CT) <: CT
            if threads && DataAPI.refpool(_columns(ds)[colsidx[j]]) === nothing
                _hp_map!_a_function!(_columns(ds)[colsidx[j]], f[j])
            else
                map!(f[j], _columns(ds)[colsidx[j]], _columns(ds)[colsidx[j]])
            end
            # removeformat!(ds, colsidx[j])
            _modified(_attributes(ds))
            if !_reset_group && parentcols(index(ds), colsidx[j]) ∈ index(parent(ds)).sortedcols
                _reset_grouping_info!(ds)
                _reset_group = true
            end
        else
            if number_of_warnnings < 10
                @warn "cannot map `f` on ds[!, :$(_names(ds)[colsidx[j]])] in-place, the selected column is $(CT) and the result of calculation is $(T)"
                number_of_warnnings += 1
            elseif number_of_warnnings == 10
                @warn "more than 5 columns can not be replaced ...."
                number_of_warnnings += 1
            end
            continue
        end
    end
    return ds
end

function Base.map!(ds::AbstractDataset, f::Function, cols::MultiColumnIndex; threads = true)
    colsidx = index(ds)[cols]
    fs = repeat([f], length(colsidx))
    map!(ds, fs, colsidx; threads = threads)
end

Base.map!(ds::AbstractDataset, f::Union{Function}, col::ColumnIndex; threads = true) = map!(ds, f, [col]; threads = threads)
Base.map!(ds::AbstractDataset, f::Vector{<:Function}; threads = true) = throw(ArgumentError("the `cols` argument cannot be left blank"))


# the order of argument in `mask` is based on map, should we make it mask(ds, cols, fun)

"""
    mask(ds::AbstractDataset, f::Function, cols; mapformats = false, threads = true)
    mask(ds::AbstractDataset, f::Vector{Function}, cols; mapformats = false, threads = true)

Map `f` on each observation and return a `Bool` data set. When multiple `f` is provided, each one map to its corresponding column.

# Examples

```jldoctest
julia> ds = Dataset(x = [1, -1, -1],
                    y = [2, 4, 1],
                    z = [1, 2, 3])
3×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64     Int64     Int64
─────┼──────────────────────────────
   1 │        1         2         1
   2 │       -1         4         2
   3 │       -1         1         3

julia> mask(ds, [>(0), iseven], 1:2)
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Bool      Bool
─────┼────────────────────
   1 │     true      true
   2 │    false      true
   3 │    false     false

julia> mask(ds, isodd, 1:3)
3×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Bool      Bool      Bool
─────┼──────────────────────────────
   1 │     true     false      true
   2 │     true     false     false
   3 │     true      true      true
```
"""
mask(ds::AbstractDataset, f::Function, col::ColumnIndex; mapformats = true, threads = true, missings = false) = mask(ds, f, [col]; mapformats = mapformats, threads = threads, missings = missings)
function mask(ds::AbstractDataset, f::Function, cols::MultiColumnIndex; mapformats = true, threads = true, missings = false)
  colsidx = index(ds)[cols]
  v_f = Vector{Function}(undef, length(colsidx))
  fill!(v_f, f)
  mask(ds, v_f, cols; mapformats = mapformats, threads = threads, missings = missings)
end

function mask(ds::AbstractDataset, f::Vector{<:Function}, cols::MultiColumnIndex; mapformats = true, threads = true, missings = false)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "the number of functions and number of cols must match"
    vs = AbstractVector[]
    for j in 1:length(colsidx)
        v = _columns(ds)[colsidx[j]]
        _col_f = getformat(ds, colsidx[j])
        fv = _our_vect_alloc(Union{Missing, Bool}, nrow(ds))
        if mapformats
          _fill_mask!(fv, v, _col_f, f[j], threads, missings)
        else
          _fill_mask!(fv, v, f[j], threads, missings)
        end
        push!(vs, fv)
    end
    Dataset(vs, _names(ds)[colsidx], copycols=false)
end

function _fill_mask!(fv, v, format, fj, threads, missings)
  if threads
    Threads.@threads for i in 1:length(fv)
      fv[i] = _bool_mask(fj)(format(v[i]))
    end
    if !ismissing(missings)
        Threads.@threads for i in 1:length(fv)
          ismissing(fv[i]) ? fv[i] = missings : nothing
        end
    end
  else
    map!(_bool_mask(fj∘format), fv, v)
    if !ismissing(missings)
        map!(x->ismissing(x) ? x = missings : x, fv, fv)
    end
  end
end
# not using formats
function _fill_mask!(fv, v, fj, threads, missings)
  if threads
    Threads.@threads for i in 1:length(fv)
      fv[i] = _bool_mask(fj)(v[i])
    end
    if !ismissing(missings)
        Threads.@threads for i in 1:length(fv)
          ismissing(fv[i]) ? fv[i] = missings : nothing
        end
    end
  else
    map!(_bool_mask(fj), fv, v)
    if !ismissing(missings)
        map!(x->ismissing(x) ? x = missings : x, fv, fv)
    end
  end
end
_bool_mask(f) = x->f(x)::Union{Bool, Missing}

# TODO is it faster to use this function for unique(..., keep = :none) case?
function _unique_none_case(ds::Dataset, cols; mapformats = false)
    if ncol(ds) == 0
        throw(ArgumentError("finding duplicate rows in data set with no " *
                            "columns is not allowed"))
    end
    groups, gslots, ngroups = _gather_groups(ds, cols, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64), mapformats = mapformats, stable = true)
    _find_groups_with_more_than_one_observation(groups, ngroups)
end

# Modify Dataset
function Base.unique!(ds::Dataset; mapformats = false, keep = :first, threads = true)
    !(keep in (:first, :last, :none, :only, :random)) && throw(ArgumentError( "The `keep` keyword argument must be one of :first, :last, :only, :none, or :random"))
    if keep == :none
        rowidx = duplicates(ds, mapformats = mapformats, leave = :or, threads = threads)
    elseif keep == :only
        rowidx = duplicates(ds, mapformats = mapformats, leave = :none, threads = threads)
    elseif keep == :random
        rowidx = duplicates(ds, mapformats = mapformats, leave = :random, threads = threads)
    else
        rowidx = duplicates(ds, mapformats = mapformats, leave = keep, threads = threads)
    end
    deleteat!(ds, rowidx)
end
# this is for fixing ambiguity
function Base.unique!(ds::Dataset, cols::AbstractVector; mapformats = false, keep = :first, threads = true)
    !(keep in (:first, :last, :none, :only, :random)) && throw(ArgumentError( "The `keep` keyword argument must be one of :first, :last, :only, :none, or :random"))
    if keep == :none
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = :or, threads = threads)
    elseif keep == :only
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = :none, threads = threads)
    elseif keep == :random
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = :random, threads = threads)
    else
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = keep, threads = threads)
    end
    deleteat!(ds, rowidx)
end
function Base.unique!(ds::Dataset, cols; mapformats = false, keep = :first, threads = true)
    !(keep in (:first, :last, :none, :only, :random)) && throw(ArgumentError( "The `keep` keyword argument must be one of :first, :last, :only, :none, or :random"))
    if keep == :none
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = :or, threads = threads)
    elseif keep == :only
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = :none, threads = threads)
    elseif keep == :random
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = :random, threads = threads)
    else
        rowidx = duplicates(ds, cols, mapformats = mapformats, leave = keep, threads = threads)
    end
    deleteat!(ds, rowidx)
end


# Unique rows of an Dataset.
@inline function Base.unique(ds::AbstractDataset; view::Bool=false, mapformats = false, keep = :first, threads = true)
    !(keep in (:first, :last, :none, :only, :random)) && throw(ArgumentError( "The `keep` keyword argument must be one of :first, :last, :only, :none, or :random"))
    if keep == :none
        rowidxs = duplicates(ds, mapformats = mapformats, leave = :none, threads = threads)
    elseif keep == :only
        rowidxs = duplicates(ds, mapformats = mapformats, leave = :or, threads = threads)
    elseif keep == :random
        rowidxs = .!duplicates(ds, mapformats = mapformats, leave = :random, threads = threads)
    else
        rowidxs = (!).(duplicates(ds, mapformats = mapformats, leave = keep, threads = threads))
    end
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end

@inline function Base.unique(ds::AbstractDataset, cols; view::Bool=false, mapformats = false, keep = :first, threads = true)
    !(keep in (:first, :last, :none, :only, :random)) && throw(ArgumentError( "The `keep` keyword argument must be one of :first, :last, :only, :none, or :random"))
    if keep == :none
        rowidxs = duplicates(ds, cols, mapformats = mapformats, leave = :none, threads = threads)
    elseif keep == :only
        rowidxs = duplicates(ds, cols, mapformats = mapformats, leave = :or, threads = threads)
    elseif keep == :random
        rowidxs = .!duplicates(ds, cols, mapformats = mapformats, leave = :random, threads = threads)
    else
        rowidxs = (!).(duplicates(ds, cols, mapformats = mapformats, leave = keep, threads = threads))
    end
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end


"""
    unique(ds::AbstractDataset, cols = : ; [mapformats = false, keep = :first, view::Bool=false, threads])
    unique!(ds::Dataset, cols = : ; [mapformats = false, keep = :first, threads])

Filter a data set based on uniqueness of its rows, and the `keep` keyword argument determines which occurance of duplicated rows should be retained. `keep` can be one of the following values: `:first`, `:last`, `:none`, `:only`, or `:random`.

* `keep = :first` the first occurrence of the duplicated rows are kept
* `keep = :last` the last occurrence are kept
* `keep = :none` all duplicates are dropped from the result
* `keep = :only` only duplicated rows are kept,
* `keep = :random` a random occurance of duplicates rows are kept

When `cols` is specified, the unique occurrence is detemined by given combination of values
in selected columns. `cols` can be any column selector.

For `unique`, if `view=false` a freshly allocated `Dataset` is returned,
and if `view=true` a `SubDataset` view into `ds` is returned.

`unique!` updates `ds` in-place and does not support the `view` keyword argument.

See also [`duplicates`](@ref).

# Arguments
- `ds` :  a data set
- `cols` :  column indicator (Symbol, Int, Vector{Symbol}, Regex, etc.) specifying the column(s) to compare.

## keyword arguments
- `mapformats` : if `true` then uniqueness of values is determined by their formatted values.
- `keep` : indicates which occurrence of the duplicate rows should be kept
- `view` : available only for `unique`, and indicates the result be a view of the input data set
- `threads` : determine if mulit-threading should be disabled

# Examples
```jldoctest
julia> ds = Dataset(i = 1:4, x = [1, 2, 1, 2])
4×2 Dataset
 Row │ i         x
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        3         1
   4 │        4         2

julia> ds = vcat(ds, ds)
8×2 Dataset
 Row │ i         x
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        3         1
   4 │        4         2
   5 │        1         1
   6 │        2         2
   7 │        3         1
   8 │        4         2

julia> unique(ds)   # doesn't modify ds
4×2 Dataset
 Row │ i         x
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        3         1
   4 │        4         2

julia> unique(ds, 2)
2×2 Dataset
 Row │ i         x
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2

julia> unique(ds, 2, keep = :last)
2×2 Dataset
 Row │ i         x
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        3         1
   2 │        4         2

julia> unique!(ds)  # modifies ds
4×2 Dataset
 Row │ i         x
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        3         1
   4 │        4         2
```
"""
(unique, unique!)


"""
    dropmissing!(ds::Dataset, cols=:; mapformats = false, threads)

Remove rows with missing values from data set `ds` and return it.

If `cols` is provided, only missing values in the corresponding columns are considered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

See also: [`dropmissing`](@ref), [`completecases`](@ref), [`byrow`](@ref), [`filter`](@ref), [`filter!`](@ref).

```jldoctest
julia> ds = Dataset(i = 1:5,
                             x = [missing, 4, missing, 2, 1],
                             y = [missing, missing, "c", "d", "e"])
5×3 Dataset
 Row │ i         x         y
     │ identity  identity  identity
     │ Int64?    Int64?    String?
─────┼──────────────────────────────
   1 │        1   missing  missing
   2 │        2         4  missing
   3 │        3   missing  c
   4 │        4         2  d
   5 │        5         1  e

julia> dropmissing!(copy(ds))
2×3 Dataset
 Row │ i         x         y
     │ identity  identity  identity
     │ Int64?    Int64?    String?
─────┼──────────────────────────────
   1 │        4         2  d
   2 │        5         1  e

julia> dropmissing!(copy(ds), :x)
3×3 Dataset
 Row │ i         x         y
     │ identity  identity  identity
     │ Int64?    Int64?    String?
─────┼──────────────────────────────
   1 │        2         4  missing
   2 │        4         2  d
   3 │        5         1  e

julia> dropmissing!(ds, [:x, :y])
2×3 Dataset
 Row │ i         x         y
     │ identity  identity  identity
     │ Int64?    Int64?    String?
─────┼──────────────────────────────
   1 │        4         2  d
   2 │        5         1  e

```
"""
function dropmissing!(ds::Dataset,
                      cols::Union{ColumnIndex, MultiColumnIndex}=:; mapformats = false, threads = nrow(ds)>Threads.nthreads() *10)
    inds = completecases(ds, cols; mapformats = mapformats, threads = threads)
    inds .= .!(inds)
    deleteat!(ds, inds)
    ds
end


"""
    describe(ds::AbstractDataset; cols=:, threads = true)
    describe(ds::AbstractDataset, fun...; cols=:, threads = true)

Return descriptive statistics for a data set as a new `Dataset`
where each row represents a variable and each column a summary statistic.

# Arguments
- `ds` : the `AbstractDataset`
- `fun...` : list of functions for summarising each column.
- `cols` : a keyword argument allowing to select only a subset
  of columns from `ds` to describe.

# Details
`IMD.n` and `IMD.nmissing` are two special functions for reporting number of nonmissing and number of missing observations, respectively.

# Examples
```jldoctest
julia> ds = Dataset(i=1:10, x=0.1:0.1:1.0, y='a':'j');

julia> describe(ds)
3×7 Dataset
 Row │ column    n         nmissing  mean      std       minimum   maximum
     │ identity  identity  identity  identity  identity  identity  identity
     │ String?   Any       Any       Any       Any       Any       Any
─────┼──────────────────────────────────────────────────────────────────────
   1 │ i         10        0         5.5       3.02765   1         10
   2 │ x         10        0         0.55      0.302765  0.1       1.0
   3 │ y         10        0         nothing   nothing   a         j

julia> describe(ds, minimum, maximum, IMD.n)
3×4 Dataset
 Row │ column    minimum   maximum   n
     │ identity  identity  identity  identity
     │ String?   Any       Any       Any
─────┼────────────────────────────────────────
   1 │ i         1         10        10
   2 │ x         0.1       1.0       10
   3 │ y         a         j         10

julia> describe(ds, sum, cols=:x)
1×2 Dataset
 Row │ column    sum
     │ identity  identity
     │ String?   Union…?
─────┼────────────────────
   1 │ x         5.5
```
"""
function DataAPI.describe(ds::Dataset,
                 stats::Base.Callable...;
                 cols=:, threads = true)
        colsidx = index(ds)[cols]

        n_stats = Vector{Base.Callable}(undef, length(stats))
        for j in 1:length(n_stats)
            n_stats[j] = x->try stats[j](x) catch  end
        end
        res = [Dataset() for _ in 1:length(colsidx)]
        newds = Dataset()

        @_threadsfor threads for j in 1:length(colsidx)
            res[j] = combine_ds(ds, colsidx[j] .=> n_stats .=> Symbol.([stats...]))
        end

        for j in 1:length(colsidx)
            append!(newds, res[j], promote = true)
        end
        varnames = (names(ds)[colsidx])
        # varnames = repeat(varnames, inner = _ngroups(ds))
        insertcols!(newds, 1, :column => varnames)
        newds
end
DataAPI.describe(ds::Dataset; cols = :) = describe(ds, n, nmissing, mean, std, minimum, maximum; cols = cols)
nmissing(x) = count(ismissing, x)
n(x) = count(!ismissing, x)

"""
    filter(ds::AbstractDataset, cols; [type = all,...])

A convenient shortcut for `ds[byrow(ds, type, cols; ...), :]`.

`type` can be any function supported by `byrow` which returns a Vector{Bool} or BitVector.

See [`byrow`](@ref), [`filter!`](@ref), [`delete!`](@ref), [`delete`](@ref)

# Examples

```jldoctest
julia> ds = Dataset(x = [1,2,3,4,5], y = [1.5,2.3,-1,0,2.0], z = Bool[1,0,1,0,1])
5×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Float64?  Bool?
─────┼──────────────────────────────
   1 │        1       1.5      true
   2 │        2       2.3     false
   3 │        3      -1.0      true
   4 │        4       0.0     false
   5 │        5       2.0      true

julia> filter(ds, :z)
3×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Float64?  Bool?
─────┼──────────────────────────────
   1 │        1       1.5      true
   2 │        3      -1.0      true
   3 │        5       2.0      true

julia> filter(ds, 1:2, by = [iseven, >(2.0)])
1×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Float64?  Bool?
─────┼──────────────────────────────
   1 │        2       2.3     false

julia> filter(ds, 1:2, type = any, by = [iseven, >(2.0)])
2×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Float64?  Bool?
─────┼──────────────────────────────
   1 │        2       2.3     false
   2 │        4       0.0     false

julia> filter(ds, 1:3, type = issorted, rev = true)
2×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Float64?  Bool?
─────┼──────────────────────────────
   1 │        4       0.0     false
   2 │        5       2.0      true

julia> filter(ds, 2:3, type = isless, with = :x)
3×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Float64?  Bool?
─────┼──────────────────────────────
   1 │        3      -1.0      true
   2 │        4       0.0     false
   3 │        5       2.0      true
```
"""
function Base.filter(ds::AbstractDataset, cols::Union{Vector{T}, ColumnIndex, MultiColumnIndex}; view = false, type= all, kwargs...) where T <: Union{<:Integer, Symbol, AbstractString}
    if view
        Base.view(ds, byrow(ds, type, cols; kwargs...), :)
    else
        ds[byrow(ds, type, cols; kwargs...), :]
    end
end
"""
    filter!(ds::AbstractDataset, cols; [type = all, ...])

Variant of `filter` which replaces the passed data set with the filtered one.

It is a convenient shortcut for `deleteat![ds, .!byrow(ds, type, cols; ...)]`.

`type` can be any function supported by `byrow` which returns a Vector{Bool} or BitVector.

Refer to [`filter`](@ref) for exmaples.

See [`byrow`](@ref), [`filter`](@ref), [`delete!`](@ref), [`delete`](@ref)
"""
Base.filter!(ds::Dataset, cols::Union{Vector{T}, ColumnIndex, MultiColumnIndex}; type = all, kwargs...) = deleteat!(ds, .!byrow(ds, type, cols; kwargs...)) where T <: Union{<:Integer, Symbol, AbstractString}

# filter out `true`s
"""
    delete(ds::AbstractDataset, cols; [type = all,...])

A convenient shortcut for `ds[.!byrow(ds, type, cols; ...), :]`.

`type` can be any function supported by `byrow` which returns a Vector{Bool} or BitVector.

Compare to [`deleteat!`](@ref)

See [`delete!`](@ref), [`byrow`](@ref), [`filter!`](@ref), [`filter`](@ref)

# Examples

```jldoctest
julia> ds = Dataset(x = [1,2,3,4,5], y = [1.5,2.3,-1,0,2.0], z = Bool[1,0,1,0,1])
5×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Float64?  Bool?
─────┼──────────────────────────────
   1 │        1       1.5      true
   2 │        2       2.3     false
   3 │        3      -1.0      true
   4 │        4       0.0     false
   5 │        5       2.0      true

julia> delete(ds, :z)
2×3 Dataset
 Row │ x         y         z        
     │ identity  identity  identity 
     │ Int64?    Float64?  Bool?    
─────┼──────────────────────────────
   1 │        2       2.3     false
   2 │        4       0.0     false

julia> delete(ds, 1:2, by = [iseven, >(2.0)])
4×3 Dataset
 Row │ x         y         z        
     │ identity  identity  identity 
     │ Int64?    Float64?  Bool?    
─────┼──────────────────────────────
   1 │        1       1.5      true
   2 │        3      -1.0      true
   3 │        4       0.0     false
   4 │        5       2.0      true

julia> delete(ds, 1:2, type = any, by = [iseven, >(2.0)])
3×3 Dataset
 Row │ x         y         z        
     │ identity  identity  identity 
     │ Int64?    Float64?  Bool?    
─────┼──────────────────────────────
   1 │        1       1.5      true
   2 │        3      -1.0      true
   3 │        5       2.0      true

julia> delete(ds, 1:3, type = issorted, rev = true)
3×3 Dataset
 Row │ x         y         z        
     │ identity  identity  identity 
     │ Int64?    Float64?  Bool?    
─────┼──────────────────────────────
   1 │        1       1.5      true
   2 │        2       2.3     false
   3 │        3      -1.0      true

julia> delete(ds, 2:3, type = isless, with = :x)
2×3 Dataset
 Row │ x         y         z        
     │ identity  identity  identity 
     │ Int64?    Float64?  Bool?    
─────┼──────────────────────────────
   1 │        1       1.5      true
   2 │        2       2.3     false
```
"""
function delete(ds::AbstractDataset, cols::Union{ColumnIndex, MultiColumnIndex}; view = false, type= all, kwargs...)
    if view
        Base.view(ds, .!byrow(ds, type, cols; kwargs...), :)
    else
        ds[.!byrow(ds, type, cols; kwargs...), :]
    end
end
"""
    delete!(ds::AbstractDataset, cols; [type = all, ...])

Variant of `delete` which replaces the passed data set with the filtered one.

It is a convenient shortcut for `deleteat![ds, byrow(ds, type, cols; ...)]`.

`type` can be any function supported by `byrow` which returns a Vector{Bool} or BitVector.

Compare to [`deleteat!`](@ref)

Refer to [`delete`](@ref) for exmaples.

See [`delete`](@ref), [`byrow`](@ref), [`filter`](@ref), [`filter!`](@ref)
"""
Base.delete!(ds::Dataset, cols::Union{ColumnIndex, MultiColumnIndex}; type = all, kwargs...) = deleteat!(ds, byrow(ds, type, cols; kwargs...))

"""
    mapcols(ds::AbstractDataset, f, cols)
    mapcols(ds::AbstractDataset, f::Vector, cols)

Return a `Dataset` where each column in `cols` of `ds` is transformed using function `f`.
`f` must return `AbstractVector` objects all with the same length or scalars
(all values other than `AbstractVector` are considered to be a scalar).

To apply different functions on different columns pass a vector of functions.

Note that `mapcols` guarantees not to reuse the columns from `ds` in the returned
`Dataset`. If `f` returns its argument then it gets copied before being stored.

See also: [`map`](@ref), [`map!`](@ref)
"""
function mapcols(ds::AbstractDataset, f::Union{Function, Type}, cols = :)
    # note: `f` must return a consistent length
    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    colsidx = index(ds)[cols]
    for j in 1:length(colsidx)
        fv = f(_columns(ds)[colsidx[j]])
        if fv isa AbstractVector
            if seenscalar
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenvector = true
            push!(vs, fv === _columns(ds)[colsidx[j]] ? copy(fv) : fv)
        else
            if seenvector
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenscalar = true
            push!(vs, [fv])
        end
    end
    return Dataset(vs, names(ds, colsidx), copycols=false)
end
function mapcols(ds::AbstractDataset, f::Vector{T}, cols = :) where T <: Union{Function, Type}
    # note: `f` must return a consistent length
    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "Number of functions must match number of columns."
    for j in 1:length(colsidx)
        fv = f[j](_columns(ds)[colsidx[j]])
        if fv isa AbstractVector
            if seenscalar
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenvector = true
            push!(vs, fv === _columns(ds)[colsidx[j]] ? copy(fv) : fv)
        else
            if seenvector
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenscalar = true
            push!(vs, [fv])
        end
    end
    return Dataset(vs, names(ds, colsidx), copycols=false)
end

function _permute_ds_after_sort!(ds, perm; check = true, cols = :, threads = true)
    if check
        @assert nrow(ds) == length(perm) "the length of perm and the nrow of the data set must match"

        if issorted(perm)
            return ds
        end
    end
    colsidx = index(ds)[cols]
    for j in 1:length(colsidx)
        if DataAPI.refpool(_columns(ds)[colsidx[j]]) !== nothing
            # if _columns(ds)[colsidx[j] isa PooledArray
            #     pa = _columns(ds)[colsidx[j]
            #     _columns(ds)[colsidx[j] = PooledArray(PooledArrays.RefArray(_threaded_permute(pa.refs, perm)), DataAPI.invrefpool(pa), DataAPI.refpool(pa), PooledArrays.refcount(pa))
            # else
            #     # TODO must be optimised
            #     _columns(ds)[colsidx[j] = _columns(ds)[colsidx[j][perm]
            # end
            # since we don't support copycols for external usage it is safe to only permute refs
            _columns(ds)[colsidx[j]].refs = _threaded_permute(_columns(ds)[colsidx[j]].refs, perm; threads = threads)
        else
            _columns(ds)[colsidx[j]] = _threaded_permute(_columns(ds)[colsidx[j]], perm; threads = threads)
        end
    end
    _modified(_attributes(ds))
end
