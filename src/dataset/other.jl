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
    inner <= 0 && throw(ArgumentError("inner keyword argument must be greater than zero"))
    outer <= 0 && throw(ArgumentError("outer keyword argument must be greater than zero"))
    if outer == 1
        for j in 1:ncol(ds)
            _columns(ds)[j] = repeat(_columns(ds)[j], inner = Int(inner), outer = 1)
        end
        _reset_grouping_info!(ds)
        # ngroups = index(ds).ngroups[]
        # diffs = diff(index(ds).starts[1:ngroups]) .* inner
        # @show diffs
        # cumsum!(diffs, diffs)
        # @show diffs
        # for j in 2:ngroups
        #     index(ds).starts[j] = diffs[j-1]
        # end
        # @show index(ds).starts
    elseif outer > 1
        for j in 1:ncol(ds)
            _columns(ds)[j] = repeat(_columns(ds)[j], inner = Int(inner), outer = Int(outer))
        end
        _reset_grouping_info!(ds)
    end
    _modified(_attributes(ds))
    ds
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
    count <= 0 && throw(ArgumentError("count must be greater than zero"))
    repeat!(ds, inner = 1, outer = count)
    ds
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
    map(ds::AbstractDataset, f::Function, cols)
    map(ds::AbstractDataset, f::Vector{Function}, cols)

Return a copy of `ds` where cols of the new `Dataset` is the result of calling `f` on each observation. The order of columns for the new data set is the same as `ds`.
Note that `map` guarantees not to reuse the columns from `ds` in the returned
`Dataset`. If `f` returns its argument then it gets copied before being stored.
The number of functions and the number of cols must match when multiple functions is used.

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
# function Base.map(ds::Dataset, f::Function, cols::MultiColumnIndex)
#     # Create Dataset
#     ncol(ds) == 0 && return ds # skip if no columns
#     colsidx = index(ds)[cols]
#     transfer_grouping_info = !any(colsidx .∈ Ref(index(ds).sortedcols))
#     sorted_colsidx = sort(colsidx)
#     vs = AbstractVector[]
#     for j in 1:ncol(ds)
#         if insorted(j, sorted_colsidx)
#             _f = f
#         else
#             _f = identity
#         end
#         v = _columns(ds)[j]
#         T = Core.Compiler.return_type(_f, (eltype(v), ))
#         fv = Vector{T}(undef, length(v))
#         _hp_map_a_function!(fv, _f, v)
#         push!(vs, fv === v ? copy(fv) : fv)
#     end
#     if transfer_grouping_info
#         newds_index = copy(index(ds))
#     else
#         newds_index = copy(index(ds))
#         _reset_grouping_info!(newds_index)
#     end
#     # formats don't need to be transferred
#     newds = Dataset(vs, newds_index, copycols=false)
#     removeformat!(newds, cols)
#     setinfo!(newds, _attributes(ds).meta.info[])
#     return newds
#
# end
# Base.map(ds::Dataset, f::Union{Function}, col::ColumnIndex) = map(ds, f, [col])
# Base.map(ds::Dataset, f::Union{Function}) = throw(ArgumentError("the `cols` argument cannot be left blank"))

function Base.map(ds::Dataset, f::Function, cols::MultiColumnIndex; threads = true)
    colsidx = index(ds)[cols]
    fs = repeat([f], length(colsidx))
    map(ds, fs, cols; threads = threads)
end
Base.map(ds::Dataset, f::Function, col::ColumnIndex; threads = true) = map(ds, f, [col]; threads = threads)
Base.map(ds::Dataset, f::Vector{Function}; threads = true) = throw(ArgumentError("the `cols` argument cannot be left blank"))


function Base.map(ds::Dataset, f::Vector{<:Function}, cols::MultiColumnIndex; threads = true)
    # Create Dataset
    ncol(ds) == 0 && return ds # skip if no columns
    colsidx = index(ds)[cols]
    @assert length(f) ==length(colsidx) "The number of functions and the number of columns must match"
    transfer_grouping_info = !any(colsidx .∈ Ref(index(ds).sortedcols))
    sorted_colsidx = sort(colsidx)
    counter_f = 1
    vs = AbstractVector[]
    for j in 1:ncol(ds)
        if insorted(j, sorted_colsidx)
            _f = f[counter_f]
            counter_f += 1
        else
            _f = identity
        end
        v = _columns(ds)[j]
        T = Core.Compiler.return_type(_f, (eltype(v), ))
        if threads
            fv = Vector{T}(undef, length(v))
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
    map!(ds::Dataset, f::Function, cols)

Update each row of each `col` in `ds[!, cols]` in-place when `map!` return a result, and skip when it is not possible.

If `f` cannot be applied in place, use `map` for creating a copy of `ds`.

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
```
"""
# function Base.map!(ds::Dataset, f::Function, cols::MultiColumnIndex)
#     # Create Dataset
#     ncol(ds) == 0 && return ds # skip if no columns
#     colsidx = index(ds)[cols]
#     _reset_group = false
#     # TODO needs function barrier
#     number_of_warnnings = 0
#     for j in 1:length(colsidx)
#         CT = eltype(_columns(ds)[colsidx[j]])
#         # Core.Compiler.return_type cannot handle the situations like x->ismissing(x) ? 0 : x when x is missing and float, since the output of Core.Compiler.return_type is Union{Missing, Float64, Int64}
#         # we remove missing and then check the result,
#         # TODO is there any problem with this?
#         T = Core.Compiler.return_type(f, (nonmissingtype(CT),))
#         if CT >: Missing
#             T = Union{Missing, T}
#         end
#         if promote_type(T, CT) <: CT
#             _hp_map!_a_function!(_columns(ds)[colsidx[j]], f)
#             # map!(f, _columns(ds)[colsidx[j]],  _columns(ds)[colsidx[j]])
#             # removeformat!(ds, colsidx[j])
#             _modified(_attributes(ds))
#             if !_reset_group && colsidx[j] ∈ index(ds).sortedcols
#                 _reset_grouping_info!(ds)
#                 _reset_group = true
#             end
#         else
#             if number_of_warnnings < 5
#                 @warn "cannot map `f` on ds[!, :$(_names(ds)[colsidx[j]])] in-place, the selected column is $(CT) and the result of calculation is $(T)"
#                 number_of_warnnings += 1
#             elseif number_of_warnnings == 5
#                 @warn "more than 5 columns can not be replaced ...."
#                 number_of_warnnings += 1
#             end
#             continue
#         end
#     end
#     return ds
# end
#
# Base.map!(ds::Dataset, f::Union{Function}) = throw(ArgumentError("the `col` argument cannot be left blank"))

"""
    map!(ds::Dataset, f::Vector{Function}, cols)

Update each row of the jth `col` in `ds[!, cols]` in-place by calling `f[j]` on it. If in-place mapping cannot be done, the mapping is skipped.

Use `map` if the in-place operation is not possible.

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
function Base.map!(ds::Dataset, f::Vector{<:Function}, cols::MultiColumnIndex; threads = true)
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
        T = Core.Compiler.return_type(f[j], (nonmissingtype(CT),))
        T = Union{Missing, T}
        if promote_type(T, CT) <: CT
            if threads
                _hp_map!_a_function!(_columns(ds)[colsidx[j]], f[j])
            else
                map!(f[j], _columns(ds)[colsidx[j]], _columns(ds)[colsidx[j]])
            end
            # removeformat!(ds, colsidx[j])
            _modified(_attributes(ds))
            if !_reset_group && colsidx[j] ∈ index(ds).sortedcols
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

function Base.map!(ds::Dataset, f::Function, cols::MultiColumnIndex; threads = true)
    colsidx = index(ds)[cols]
    fs = repeat([f], length(colsidx))
    map!(ds, fs, colsidx; threads = threads)
end

Base.map!(ds::Dataset, f::Union{Function}, col::ColumnIndex; threads = true) = map!(ds, f, [col]; threads = threads)
Base.map!(ds::Dataset, f::Vector{<:Function}; threads = true) = throw(ArgumentError("the `cols` argument cannot be left blank"))


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
        fv = Vector{Union{Missing, Bool}}(undef, nrow(ds))
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
    Threads.@threads for i in 1:length(fv)
      ismissing(fv[i]) ? fv[i] = missings : nothing
    end
  else
    map!(_bool_mask(fj∘format), fv, v)
    map!(x->ismissing(x) ? x = missings : x, fv, fv)
  end
end
# not using formats
function _fill_mask!(fv, v, fj, threads, missings)
  if threads
    Threads.@threads for i in 1:length(fv)
      fv[i] = _bool_mask(fj)(v[i])
    end
    Threads.@threads for i in 1:length(fv)
      ismissing(fv[i]) ? fv[i] = missings : nothing
    end
  else
    map!(_bool_mask(fj), fv, v)
    map!(x->ismissing(x) ? x = missings : x, fv, fv)
  end
end
_bool_mask(f) = x->f(x)::Union{Bool, Missing}


# Unique cases

# Modify Dataset
Base.unique!(ds::Dataset; mapformats = false) = delete!(ds, nonunique(ds, mapformats = mapformats))
Base.unique!(ds::Dataset, cols::AbstractVector; mapformats = false) =
    delete!(ds, nonunique(ds, cols, mapformats = mapformats))
Base.unique!(ds::Dataset, cols; mapformats = false) =
    delete!(ds, nonunique(ds, cols, mapformats = mapformats))

# Unique rows of an Dataset.
@inline function Base.unique(ds::AbstractDataset; view::Bool=false, mapformats = false)
    rowidxs = (!).(nonunique(ds, mapformats = mapformats))
    return view ? Base.view(ds, rowidxs, :) : ds[rowidxs, :]
end

@inline function Base.unique(ds::AbstractDataset, cols; view::Bool=false, mapformats = false)
    rowidxs = (!).(nonunique(ds, cols, mapformats = mapformats))
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
    dropmissing!(ds::Dataset, cols=:; disallowmissing::Bool=true)

Remove rows with missing values from data set `ds` and return it.

If `cols` is provided, only missing values in the corresponding columns are considered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `disallowmissing` is `true` (the default) then the `cols` columns will
get converted using [`disallowmissing!`](@ref).

See also: [`dropmissing`](@ref) and [`completecases`](@ref).

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

julia> dropmissing!(copy(ds))
2×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e

julia> dropmissing!(copy(ds), disallowmissing=false)
2×3 Dataset
 Row │ i      x       y
     │ Int64  Int64?  String?
─────┼────────────────────────
   1 │     4       2  d
   2 │     5       1  e

julia> dropmissing!(copy(ds), :x)
3×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String?
─────┼───────────────────────
   1 │     2      4  missing
   2 │     4      2  d
   3 │     5      1  e

julia> dropmissing!(ds, [:x, :y])
2×3 Dataset
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e
```
"""
function dropmissing!(ds::Dataset,
                      cols::Union{ColumnIndex, MultiColumnIndex}=:)
    inds = completecases(ds, cols)
    inds .= .!(inds)
    delete!(ds, inds)
    ds
end

function compare(ds1::Dataset, ds2::Dataset; on = nothing, eq = isequal)
    if on === nothing
        left_col_idx = 1:ncol(ds1)
        right_col_idx = index(ds2)[names(ds1)]
    elseif typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        left_col_idx = index(ds1)[on]
        right_col_idx = index(ds2)[names(ds1)[left_col_idx]]
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        left_col_idx = index(ds1)[map(x->x.first, on)]
        right_col_idx = index(ds2)[map(x->x.second, on)]
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end

    nrow(ds1) != nrow(ds2) && throw(ArgumentError("the number of rows for both data sets should be the same"))
    res = Dataset()
    for j in 1:length(left_col_idx)
        _res = allocatecol(Union{Bool, Missing}, nrow(ds1))
        _res .= eq.(_columns(ds1)[left_col_idx[j]], _columns(ds2)[right_col_idx[j]])
        push!(_columns(res), _res)
        push!(index(res),  Symbol(names(ds1)[left_col_idx[j]]* "=>" * names(ds2)[right_col_idx[j]]))
    end
    res
end


function DataAPI.describe(ds::Dataset,
                 stats::Base.Callable...;
                 cols=:)
        colsidx = index(ds)[cols]

        n_stats = Vector{Base.Callable}(undef, length(stats))
        for j in 1:length(n_stats)
            n_stats[j] = x->try stats[j](x) catch  end
        end
        res = [Dataset() for _ in 1:length(colsidx)]
        newds = Dataset()

        Threads.@threads for j in 1:length(colsidx)
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
DataAPI.describe(ds::Dataset; cols = :) = describe(ds, n, nmissing, sum, mean, std, minimum, maximum; cols = :)
nmissing(x) = count(ismissing, x)
n(x) = count(!ismissing, x)
