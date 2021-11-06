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

function _fill_range_for_accelerated_join!(ranges, starts, loc, x, f, sz, chunk)
    loc_cumsum = cumsum(loc)
    Threads.@threads for i in 1:length(x)
        _index_ = hash(f(x[i])) % chunk + 1
        if loc[_index_]
            actual_start = loc_cumsum[_index_]
            st = starts[actual_start]
            actual_start == length(starts) ? en = sz : en = starts[actual_start+1]-1
            ranges[i] = st:en
        else
            ranges[i] = 1:0
        end
    end
end
# TODO how the hashing behave for Categorical Arrays?
function _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate, chunk = 2^10)
    if isempty(dsr)
        idx = []
        fill!(ranges, 1:nrow(dsr))
    else
        if accelerate
            if mapformats[2]
                _fr = getformat(dsr, oncols_right[1])
            else
                _fr = identity
            end
            grng = _divide_for_fast_join(dsr[!, oncols_right[1]].val, _fr, chunk)
            if mapformats[1]
                _fl = getformat(dsl, oncols_left[1])
            else
                _fl = identity
            end
            _fill_range_for_accelerated_join!(ranges, grng.starts, grng.starts_loc, dsl[!, oncols_left[1]].val, _fl, nrow(dsr), chunk)
            starts, idx, last_valid_range =  _sortperm(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = true, givenrange = grng)
        else
            starts, idx, last_valid_range =  _sortperm(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = true)
            fill!(ranges, 1:nrow(dsr))
        end
    end
    idx
end



function _fill_val_join!(x, r, val)
    for i in r
        x[i] = val
    end
end

function _find_ranges_for_join!(ranges, x, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        ranges[i] = searchsorted_join(_fr, y, _fl(DataAPI.unwrap(x[i]))::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
    end
end

function _find_ranges_for_join_pa!(ranges, x, invpool, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        revmap_paval_ref = get(invpool, _fl(DataAPI.unwrap(x[i]))::T1, missing)
        if ismissing(revmap_paval_ref)
            ranges[i] = 1:0
        else
            #_fr is identity
            ranges[i] = searchsorted_join(identity, y, revmap_paval_ref, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
        end
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

function _change_refpool_find_range_for_join!(ranges, dsl, dsr, r_perms, oncols_left, oncols_right, lmf, rmf, j)
    var_l = _columns(dsl)[oncols_left[j]]
    var_r = _columns(dsr)[oncols_right[j]]
    l_idx = oncols_left[j]
    r_idx = oncols_right[j]
    if lmf
        _fl = getformat(dsl, l_idx)
    else
        _fl = identity
    end
    if rmf
        _fr = getformat(dsr, r_idx)
    else
        _fr = identity
    end

    T1 = Core.Compiler.return_type(_fl∘DataAPI.unwrap, (eltype(var_l), ))

    if DataAPI.refpool(var_r) !== nothing
        # sort taken care for refs ordering of modified values, but we still need to change refs
        if _fr == identity
            var_r_cpy = var_r
        else
            var_r_cpy = map(_fr, var_r)
        end
        T2 = eltype(DataAPI.refarray(var_r_cpy))
        # now _fr must be identity
        _fr = identity
        # we should use invpool of right column
        _find_ranges_for_join_pa!(ranges, var_l, DataAPI.invrefpool(var_r_cpy), view(DataAPI.refarray(var_r_cpy), r_perms), _fl, _fr, Val(T1), Val(T2))
    else
        T2 = Core.Compiler.return_type(_fr, (eltype(var_r), ))
        _find_ranges_for_join!(ranges, var_l, view(var_r, r_perms), _fl, _fr, Val(T1), Val(T2))
    end
end



function _join_left(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false) where T
    isempty(dsl) && return copy(dsl)
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    idx = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate)
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
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
            _fill_right_cols_table_left!(_res.refs, view(_columns(dsr)[right_cols[j]].refs, idx), ranges, new_ends, total_length, fill_val)
        else
            _fill_right_cols_table_left!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length, missing)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end

function _join_left!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false) where T
    isempty(dsl) && return dsl
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    idx = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate)
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end

    if !all(x->length(x) <= 1, ranges)
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
            _fill_right_cols_table_left!(_res.refs, view(_columns(dsr)[right_cols[j]].refs, idx), ranges, new_ends, total_length, missing)
        else
            _fill_right_cols_table_left!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length, missing)
        end
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    dsl
end

function _join_inner(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false) where T
    isempty(dsl) || isempty(dsr) && throw(ArgumentError("in `innerjoin` both left and right tables must be non-empty"))
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    idx = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate)
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
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
            _fill_right_cols_table_inner!(_res.refs, view(_columns(dsr)[right_cols[j]].refs, idx), ranges, new_ends, total_length)
        else
            _fill_right_cols_table_inner!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end

function _in_use_Set(ldata, rdata, _fl, _fr)
    ss = Set(Base.Generator(_fr, rdata));
    res = Vector{Bool}(undef, length(ldata))
    Threads.@threads for i in 1:length(res)
        res[i] = _fl(ldata[i]) in ss
    end
    res
end


function _in(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, mapformats = [true, true], stable = false, alg = HeapSort, accelerate = false) where T
    isempty(dsl) && return Bool[]
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]

    # use Set when there is only one column in `on`
    if length(oncols_right) == 1
        if mapformats[1]
            _fl = getformat(dsl, oncols_left[1])
        else
            _fl = identity
        end
        if mapformats[2]
            _fr = getformat(dsr, oncols_right[1])
        else
            _fr = identity
        end
        return _in_use_Set(dsl[!, oncols_left[1]].val, dsr[!, oncols_right[1]].val, _fl, _fr)
    end
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    idx = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate)
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end
    map(x -> length(x) == 0 ? false : true, ranges)
end

function _find_right_not_in_left(ranges, n, idx)
    res = ones(Bool, n)
    Threads.@threads for i in 1:length(ranges)
        view(res, ranges[i]) .= false
    end
    findall(res)
end

function _fill_oncols_left_table_left_outer!(res, x, notinleft, en, total)
    # TODO when x is a pooled array, we cannot use Threads.@threads (maybe we should create pool first and then use Threads)
    for i in en[end]+1:total
        val = x[notinleft[i - en[end]]]
        res[i] = val
    end
end


function _join_outer(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false) where T
    isempty(dsl) || isempty(dsr) && throw(ArgumentError("in `outerjoin` both left and right tables must be non-empty"))
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    idx = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate)
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end
    new_ends = map(x -> max(1, length(x)), ranges)
    notinleft = _find_right_not_in_left(ranges, nrow(dsr), idx)
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
        _fill_oncols_left_table_left_outer!(res[oncols_left[j]], view(_columns(dsr)[oncols_right[j]], idx), notinleft, new_ends, total_length)
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, view(_columns(dsr)[right_cols[j]].refs, idx), ranges, new_ends, total_length, fill_val)
            _fill_oncols_left_table_left_outer!(_res.refs, view(_columns(dsr)[right_cols[j]].refs, idx), notinleft, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length, missing)
            _fill_oncols_left_table_left_outer!(_res, view(_columns(dsr)[right_cols[j]], idx), notinleft, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end
