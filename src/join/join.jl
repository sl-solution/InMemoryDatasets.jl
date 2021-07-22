function searchsortedfirst_join(_f, v::AbstractVector, x, lo::T, hi::T, o::Ordering)::keytype(v) where T<:Integer
    u = T(1)
    lo = lo - u
    hi = hi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        if lt(o, _f(v[m]), x)
            lo = m
        else
            hi = m
        end
    end
    return hi
end

# index of the last value of vector a that is less than or equal to x;
# returns 0 if x is less than all values of v.
function searchsortedlast_join(_f, v::AbstractVector, x, lo::T, hi::T, o::Ordering)::keytype(v) where T<:Integer
    u = T(1)
    lo = lo - u
    hi = hi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        if lt(o, x, _f(v[m]))
            hi = m
        else
            lo = m
        end
    end
    return lo
end

# returns the range of indices of v equal to x
# if v does not contain x, returns a 0-length range
# indicating the insertion point of x
function searchsorted_join(_f, v::AbstractVector, x, ilo::T, ihi::T, o::Ordering)::UnitRange{keytype(v)} where T<:Integer
    u = T(1)
    lo = ilo - u
    hi = ihi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        fvm = _f(v[m])
        if lt(o, fvm, x)
            lo = m
        elseif lt(o, x, fvm)
            hi = m
        else
            a = searchsortedfirst_join(_f, v, x, max(lo,ilo), m, o)
            b = searchsortedlast_join(_f, v, x, m, min(hi,ihi), o)
            return a : b
        end
    end
    return (lo + 1) : (hi - 1)
end

function _fill_val_join!(x, r, val)
    for i in r
        x[i] = val
    end
end

# TODO it is not working for CategroicalArrays
function _find_ranges_for_join!(ranges, x, y, _fl, _fr)
    Threads.@threads for i in 1:length(x)
        ranges[i] = searchsorted_join(_fr, y, _fl(x[i]), ranges[i].start, ranges[i].stop, Base.Order.Forward)
    end
end

function _fill_oncols_left_table_left!(_res, x, ranges, en, total)
    Threads.@threads for i in 1:length(x)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        _fill_val_join!(_res, lo:hi, x[i])
    end
    Threads.@threads for i in en[length(x)]+1:total
        _res[i] = missing
    end
end

function _fill_oncols_left_table_inner!(_res, x, ranges, en, total)
    Threads.@threads for i in 1:length(x)
        length(ranges[i]) == 0 && continue
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        _fill_val_join!(_res, lo:hi, x[i])
    end
end

function _fill_oncols_left_table_anti!(_res, x, ranges, en, total)
    Threads.@threads for i in 1:length(x)
        length(ranges[i]) != 0 && continue
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        _fill_val_join!(_res, lo:hi, x[i])
    end
end


function _fill_right_cols_table_left!(_res, x, ranges, en, total)
    Threads.@threads for i in 1:length(ranges)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        length(ranges[i]) == 0 ? _fill_val_join!(_res, lo:hi, missing) : copyto!(_res, lo, x, ranges[i].start, length(ranges[i]))
    end
end

function _fill_right_cols_table_inner!(_res, x, ranges, en, total)
    Threads.@threads for i in 1:length(ranges)
        length(ranges[i]) == 0 && continue
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        copyto!(_res, lo, x, ranges[i].start, length(ranges[i]))
    end
end

function _join_left(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, check = true) where T
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end
    new_ends = map(x -> max(1, length(x)), ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
    end
    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length)
        if DataAPI.refpool(_res) !== nothing
            _fill_oncols_left_table_left!(_res.refs, _columns(dsl)[j].refs, ranges, new_ends, total_length)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length)
        end
        push!(res, _res)
    end
    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            _fill_right_cols_table_left!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end

function _join_left!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, check = true) where T
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    _current_dsr_modified_time = _attributes(dsr).meta.modified[]
    sort!(dsr, oncols_right)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end

    if !all(x->length(x) <= 1, ranges)
        # unsort dsr
        _permute_ds_after_sort!(dsr, invperm(index(dsr).perm))
        _attributes(dsr).meta.modified[] = _current_dsr_modified_time
        throw(ArgumentError("`leftjoin!` can only be used when each observation in left data set matches at most one observation from right data set"))
    end

    new_ends = map(x -> max(1, length(x)), ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            _fill_right_cols_table_left!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length)
        end
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    dsl
end

function _join_inner(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, check = true) where T
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end
    new_ends = map(length, ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
    end
    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length)
        if DataAPI.refpool(_res) !== nothing
            _fill_oncols_left_table_inner!(_res.refs, _columns(dsl)[j].refs, ranges, new_ends, total_length)
        else
            _fill_oncols_left_table_inner!(_res, _columns(dsl)[j], ranges, new_ends, total_length)
        end
        push!(res, _res)
    end
    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            _fill_right_cols_table_inner!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, new_ends, total_length)
        else
            _fill_right_cols_table_inner!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end


# function _join_anti(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, check = true) where T
#     oncols_left = index(dsl)[onleft]
#     oncols_right = index(dsr)[onright]
#     right_cols = setdiff(1:length(index(dsr)), oncols_right)
#     # dsr_oncols = select(dsr, oncols, copycols = true)
#     sort!(dsr, oncols_right)
#     ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
#     fill!(ranges, 1:nrow(dsr))
#     for j in 1:length(oncols_left)
#         _fl = getformat(dsl, oncols_left[j])
#         _fr = getformat(dsr, oncols_right[j])
#         _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
#     end
#     new_ends = map(x -> length(x) == 0 ? 1 : 0, ranges)
#     cumsum!(new_ends, new_ends)
#     total_length = new_ends[end]
#
#     if check
#         @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
#     end
#     res = []
#     for j in 1:length(index(dsl))
#         _res = Tables.allocatecolumn(eltype(_columns(dsl)[j]), total_length)
#         _fill_oncols_left_table_anti!(_res, _columns(dsl)[j], ranges, new_ends, total_length)
#         push!(res, _res)
#     end
#     newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
#     newds
#
# end

function _in(dsl::Dataset, dsrin::Dataset, ::Val{T}; onleft, onright) where T
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsrin)[onright]
    # right_cols = setdiff(1:length(index(dsr)), oncols_right)
    # dsr_oncols = select(dsr, oncols, copycols = true)
    dsr = sort!(dsrin[!, oncols_right], :)
    oncols_right = 1:length(oncols_right)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end
    map(x -> length(x) == 0 ? false : true, ranges)
end

function _find_right_not_in_left(ranges, n)
    res = ones(Bool, n)
    Threads.@threads for i in 1:length(ranges)
        view(res, ranges[i]) .= false
    end
    findall(res)
end

function _fill_oncols_left_table_left_outer!(res, x, notinleft, en, total)
    Threads.@threads for i in en[end]+1:total
        val = x[notinleft[i - en[end]]]
        res[i] = val
    end
end


function _join_outer(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, check = true) where T
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end
    new_ends = map(x -> max(1, length(x)), ranges)
    notinleft = _find_right_not_in_left(ranges, nrow(dsr))
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end] + length(notinleft)

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
    end
    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length)
        if DataAPI.refpool(_res) !== nothing
            _fill_oncols_left_table_left!(_res.refs, _columns(dsl)[j].refs, ranges, new_ends, total_length)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length)
        end
        push!(res, _res)
    end

    for j in 1:length(oncols_left)
        # TODO is it possible to use refs of pooled array for this (there are two different columns and the refs may not be the same)
        _fill_oncols_left_table_left_outer!(res[oncols_left[j]], _columns(dsr)[oncols_right[j]], notinleft, new_ends, total_length)
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            _fill_right_cols_table_left!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, new_ends, total_length)
            _fill_oncols_left_table_left_outer!(_res.refs, _columns(dsr)[right_cols[j]].refs, notinleft, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length)
            _fill_oncols_left_table_left_outer!(_res, _columns(dsr)[right_cols[j]], notinleft, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end
