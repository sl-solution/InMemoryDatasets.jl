function searchsortedfirst_join(_f, v::AbstractVector, x, lo::T, hi::T, o::Ordering, ::Val{T2})::keytype(v) where T<:Integer where T2
    u = T(1)
    lo = lo - u
    hi = hi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        fvm = _f(v[m])::T2
        if lt(o, fvm , x)
            lo = m
        else
            hi = m
        end
    end
    return hi
end

# index of the last value of vector a that is less than or equal to x;
# returns 0 if x is less than all values of v.
function searchsortedlast_join(_f, v::AbstractVector, x, lo::T, hi::T, o::Ordering, ::Val{T2})::keytype(v) where T<:Integer where T2
    u = T(1)
    lo = lo - u
    hi = hi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        fvm = _f(v[m])::T2
        if lt(o, x, fvm)
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
function searchsorted_join(_f, v::AbstractVector, x, ilo::T, ihi::T, o::Ordering, ::Val{T2})::UnitRange{keytype(v)} where T<:Integer where T2
    u = T(1)
    lo = ilo - u
    hi = ihi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        fvm = _f(v[m])::T2
        if lt(o, fvm, x)
            lo = m
        elseif lt(o, x, fvm)
            hi = m
        else
            a = searchsortedfirst_join(_f, v, x, max(lo,ilo), m, o, Val(T2))
            b = searchsortedlast_join(_f, v, x, m, min(hi,ihi), o, Val(T2))
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

function _find_ranges_for_join!(ranges, x, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        ranges[i] = searchsorted_join(_fr, y, _fl(x[i])::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
    end
end

function _fill_oncols_left_table_left!(_res, x, ranges, en, total, fill_val)
    Threads.@threads for i in 1:length(x)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        _fill_val_join!(_res, lo:hi, x[i])
    end
    Threads.@threads for i in en[length(x)]+1:total
        _res[i] = fill_val
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


function _fill_right_cols_table_left!(_res, x, ranges, en, total, fill_val)
    Threads.@threads for i in 1:length(ranges)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        length(ranges[i]) == 0 ? _fill_val_join!(_res, lo:hi, fill_val) : copyto!(_res, lo, x, ranges[i].start, length(ranges[i]))
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

function _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, lmf, rmf, j)
    var_l = _columns(dsl)[oncols_left[j]]
    var_r = _columns(dsr)[oncols_right[j]]
    l_idx = oncols_left[j]
    r_idx = oncols_right[j]
    if lmf
        format_l = getformat(dsl, l_idx)
    else
        format_l = identity
    end
    if rmf
        format_r = getformat(dsr, r_idx)
    else
        format_r = identity
    end
    # TODO this is not very elegant code
    # the reason for this is that for Categorical array we need to translate Categorical values to actual values
    # but this is not a good idea for PooledArray (currently I just use a way to fix this)
    # the type annotation is not also very acceptable (I am not sure it is needed here??)
    # FIXME optimisation is required for Characters type (there are many allocations when used with Pooled arrays or Cat Array)
    if DataAPI.refpool(var_l) !== nothing && !(var_l isa PooledArray) && DataAPI.refpool(var_r) !== nothing && !(var_r isa PooledArray)
        dict_l = _generate_inverted_dict_pool(var_l)
        dict_r = _generate_inverted_dict_pool(var_r)
        T1 = Core.Compiler.return_type(format_l, (valtype(dict_l), ))
        T2 = Core.Compiler.return_type(format_r, (valtype(dict_r), ))
        _fl = x -> format_l(dict_l[x])
        _fr = x -> format_r(dict_r[x])
        _find_ranges_for_join!(ranges, var_l.refs, var_r.refs, _fl, _fr, Val(T1), Val(T2))
    elseif DataAPI.refpool(var_l) !== nothing && !(var_l isa PooledArray)
        dict_l = _generate_inverted_dict_pool(var_l)
        T1 = Core.Compiler.return_type(format_l, (valtype(dict_l), ))
        T2 = Core.Compiler.return_type(format_r, (eltype(var_r), ))
        _fl = x -> format_l(dict_l[x])
        _fr = format_r
        _find_ranges_for_join!(ranges, var_l.refs, var_r, _fl, _fr, Val(T1), Val(T2))
    elseif DataAPI.refpool(var_r) !== nothing && !(var_r isa PooledArray)
        dict_r = _generate_inverted_dict_pool(var_r)
        T2 = Core.Compiler.return_type(format_r, (valtype(dict_r), ))
        T1 = Core.Compiler.return_type(format_l, (eltype(var_l), ))
        _fl = format_l
        _fr = x -> format_r(dict_r[x])
        _find_ranges_for_join!(ranges, var_l, var_r.refs, _fl, _fr, Val(T1), Val(T2))
    else
        T1 = Core.Compiler.return_type(format_l, (eltype(var_l), ))
        T2 = Core.Compiler.return_type(format_r, (eltype(var_r), ))
        _fl = format_l
        _fr = format_r
        _find_ranges_for_join!(ranges, var_l, var_r, _fl, _fr, Val(T1), Val(T2))
    end
end


function _join_left(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, check = true) where T
    isempty(dsl) && return copy(dsl)
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right, stable = stable)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end
    new_ends = map(x -> max(1, length(x)), ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
    end
    res = []
    for j in 1:length(index(dsl))
        addmissing = false
        _res = allocatecol(_columns(dsl)[j], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            # fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_oncols_left_table_left!(_res.refs, _columns(dsl)[j].refs, ranges, new_ends, total_length, missing)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing)
        end
        push!(res, _res)
    end
    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, new_ends, total_length, fill_val)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end

function _join_left!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, check = true) where T
    isempty(dsl) && return dsl
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    _current_dsr_modified_time = _attributes(dsr).meta.modified[]
    sort!(dsr, oncols_right, stable = stable)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
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
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            # fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, new_ends, total_length, missing)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing)
        end
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    dsl
end

function _join_inner(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, check = true) where T
    isempty(dsl) || isempty(dsr) && throw(ArgumentError("in `innerjoin` both left and right tables must be non-empty"))
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right, stable = stable)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end
    new_ends = map(length, ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
    end
    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            _fill_oncols_left_table_inner!(_res.refs, _columns(dsl)[j].refs, ranges, new_ends, total_length)
        else
            _fill_oncols_left_table_inner!(_res, _columns(dsl)[j], ranges, new_ends, total_length)
        end
        push!(res, _res)
    end
    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length, addmissing = false)
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

function _in(dsl::Dataset, dsrin::Dataset, ::Val{T}; onleft, onright, mapformats = [true, true], stable = false) where T
    isempty(dsl) && return Bool[]
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsrin)[onright]
    # right_cols = setdiff(1:length(index(dsr)), oncols_right)
    # dsr_oncols = select(dsr, oncols, copycols = true)
    dsrperm = sortperm(dsrin, oncols_right, stable = stable)
    dsr = dsrin[dsrperm, oncols_right]
    # dsr = sort!(dsrin[!, oncols_right], :)
    oncols_right = 1:length(oncols_right)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
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


function _join_outer(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, check = true) where T
    isempty(dsl) || isempty(dsr) && throw(ArgumentError("in `outerjoin` both left and right tables must be non-empty"))
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right, stable = stable)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
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
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_oncols_left_table_left!(_res.refs, _columns(dsl)[j].refs, ranges, new_ends, total_length, fill_val)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing)
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
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, new_ends, total_length, fill_val)
            _fill_oncols_left_table_left_outer!(_res.refs, _columns(dsr)[right_cols[j]].refs, notinleft, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing)
            _fill_oncols_left_table_left_outer!(_res, _columns(dsr)[right_cols[j]], notinleft, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end
