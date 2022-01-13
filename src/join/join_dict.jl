function _create_dictionary_for_join_general(f, v, ::Val{T}) where T
    flag = false
    n = length(v)
    sz = max(1 + ((5 * n) >> 2), 16)
    sz = 1 << (8 * sizeof(sz) - leading_zeros(sz - 1))
    @assert 4 * sz >= 5 * n
    gslots = zeros(T, sz)
    szm1 = sz - 1
    maxprobe = 0
    @inbounds for i in 1:length(v)
        _fv = f(v[i])
        slotix = hash(_fv) & szm1 + 1
        probe = 0
        while true
            g_row = gslots[slotix]
            if g_row == 0
                gslots[slotix] = i
                break
            elseif isequal(_fv, f(v[g_row]))
                flag = true
                return gslots, maxprobe, sz, flag, 1
            end
            slotix = slotix & szm1 + 1
            probe += 1
            @assert probe < sz
        end
        if probe > maxprobe
            maxprobe = probe
        end
    end
    gslots, maxprobe, sz, flag, 1
end

function _create_dictionary_for_join_int(f, v, minval, rangelen, ::Val{T}) where T
    flag = false
    offset = 1 - minval
    n = length(v)
    sz = rangelen + 1
    gslots = zeros(T, sz)
    @inbounds for i in 1:length(v)
        _fv = f(v[i])
        if ismissing(_fv)
            slotix = sz
        else
            slotix = _fv + offset
        end
        if gslots[slotix] == 0
            gslots[slotix] = i
        else
            flag = true
            break
        end
    end
    gslots, minval, sz, flag, 2
end

function _create_dictionary_for_join(f, v, fl, vl, ::Val{T}) where T
    if f == _date_value∘identity && DataAPI.refpool(v) !== nothing
        minval = hp_minimum(DataAPI.refarray(v))
        maxval = hp_maximum(DataAPI.refarray(v))
        rangelen = maxval - minval + 1
        _create_dictionary_for_join_int(identity, DataAPI.refarray(v), minval, rangelen, Val(T))
    elseif nonmissingtype(return_type(f, v)) <: AbstractVector{<:Union{Missing, INTEGERS}} && nonmissingtype(return_type(fl, vl)) <: AbstractVector{<:Union{Missing, INTEGERS}}
        minval = hp_minimum(f, v)
        # if minval is missing all values are missing
        if ismissing(minval)
            return _create_dictionary_for_join_general(f, v, Val(T))
        end
        maxval::Integer = hp_maximum(f, v)
        rnglen = BigInt(maxval) - BigInt(minval) + 1
        o1 = false
        if rnglen < typemax(Int)
            o1 = true
            rangelen = Int(rnglen)
        end
        if o1 && maxval < typemax(Int) && (rangelen < 2.0*length(v))
            _create_dictionary_for_join_int(f, v, Int(minval), Int(rangelen), Val(T))
        else
            _create_dictionary_for_join_general(f, v, Val(T))
        end
    else
        _create_dictionary_for_join_general(f, v, Val(T))
    end
end



function _query_dictionary_for_join_general(f, r_v, fv, gslots, maxprobe, sz)
    szm1 = sz - 1
    slotix = hash(fv) & szm1 + 1
    probe = 0
    @inbounds while true
        g_row = gslots[slotix]
        g_row == 0 && break
        if isequal(fv, f(r_v[g_row]))
            return g_row
        end
        slotix = slotix & szm1 + 1
        probe += 1
        probe > maxprobe && break
    end
    0
end
function _query_dictionary_for_join_int(f, r_v, fv, gslots, minval, sz)
    offset = 1 - minval
    if ismissing(fv)
        slotix = sz
    else
        slotix = fv + offset
        !(slotix in 1:sz-1) && return 0
    end

    if slotix in 1:sz
        rowid = gslots[slotix]
        if rowid == 0
            return 0
        else
            return rowid
        end
    end
    0
end

function _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, x_l, x_r, sz, type; threads = true)
    if _fr == _date_value∘identity && DataAPI.refpool(x_r) !== nothing
        invp = DataAPI.invrefpool(x_r)
        @_threadsfor threads for i in 1:length(x_l)
            revmap_paval_ref = get(invp, DataAPI.unwrap(_fl(x_l[i])), missing)
            loc = _query_dictionary_for_join_int(_fr, x_r,revmap_paval_ref, dict, maxprob, sz)
            if loc == 0
                ranges[i] = 1:0
            else
                ranges[i] = loc:loc
            end
        end
    elseif return_type(_fr, x_r) <: AbstractVector{<:Union{Missing, INTEGERS}} && type == 2
        @_threadsfor threads for i in 1:length(x_l)
            loc = _query_dictionary_for_join_int(_fr, x_r, _fl(x_l[i]), dict, maxprob, sz)
            if loc == 0
                ranges[i] = 1:0
            else
                ranges[i] = loc:loc
            end
        end
    elseif type == 1
        @_threadsfor threads for i in 1:length(x_l)
            loc = _query_dictionary_for_join_general(_fr, x_r, _fl(x_l[i]), dict, maxprob, sz)
            if loc == 0
                ranges[i] = 1:0
            else
                ranges[i] = loc:loc
            end
        end
    end
end


function _find_ranges_for_join_using_hash(dsl, dsr, onleft, onright, mapformats, makeunique, ::Val{T}; threads = true) where T
    oncols_left = onleft
    oncols_right = onright
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end

    cols = Any[]
    for j in 1:length(oncols_left)
        if mapformats[1]
            fl = getformat(dsl, oncols_left[j])
        else
            fl = identity
        end
        if mapformats[2]
            fr = getformat(dsr, oncols_right[j])
        else
            fr = identity
        end
        push!(cols, Cat2Vec(_columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], fl, fr))
    end
    newds = Dataset(cols, :auto, copycols = false)
    a = _gather_groups(newds, :, nrow(newds)< typemax(Int32) ? Val(Int32) : Val(Int64), stable = false, mapformats = false, threads = threads)

    reps = _find_counts_for_join(view(a[1], nrow(dsl)+1:length(a[1])), a[3])
    gslots, minval, sz = _create_dictionary_for_join_int(identity, view(a[1], nrow(dsl)+1:length(a[1])), reps, 1, a[3], Val(T))

    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    where = Vector{T}(undef, length(reps)+1)
    cumsum!(view(where, 2:length(where)), reps)
    where[1] = 0
    _find_range_for_join!(ranges, view(a[1], 1:nrow(dsl)), gslots, reps, where, 1, sz, threads = threads)
    ranges, a, gslots, minval, reps, sz, right_cols
end

function _join_left_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check, threads = true ) where T
    _fl = _date_value∘identity
    _fr = _date_value∘identity
    if mapformats[1]
        _fl = _date_value∘getformat(dsl, onleft[1])
    end
    if mapformats[2]
        _fr = _date_value∘getformat(dsr, onright[1])
    end
    dict, maxprob, sz, fallback, type = _create_dictionary_for_join(_fr, _columns(dsr)[onright[1]], _fl, _columns(dsl)[onleft[1]], Val(T))
    # key is not unique, fall back to sort
    if fallback
        return false, Dataset()
    end

    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type, threads = threads)

    new_ends = map(x -> max(1, length(x)), ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]
    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end
    res = []
    for j in 1:length(index(dsl))
        addmissing = false
        _res = allocatecol(_columns(dsl)[j], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            # fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_oncols_left_table_left!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length, missing, threads = threads)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing, threads = threads)
        end
        push!(res, _res)

    end
    if dsl isa SubDataset
        newds = Dataset(res, copy(index(dsl)), copycols = false)
    else
        newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length, fill_val, threads = threads)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing, threads = threads)
        end
        push!(_columns(newds), _res)

        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    true, newds

end

function _join_left!_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check, threads = true ) where T
    _fl = _date_value∘identity
    _fr = _date_value∘identity
    if mapformats[1]
        _fl = _date_value∘getformat(dsl, onleft[1])
    end
    if mapformats[2]
        _fr = _date_value∘getformat(dsr, onright[1])
    end
    dict, maxprob, sz, fallback, type = _create_dictionary_for_join(_fr, _columns(dsr)[onright[1]], _fl, _columns(dsl)[onleft[1]], Val(T))
    # key is not unique, fall back to sort
    if fallback
        return false, Dataset()
    end

    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type, threads = threads)
    if !all(x->length(x) <= 1, ranges)
        throw(ArgumentError("`leftjoin!` can only be used when each observation in left data set matches at most one observation from right data set"))
    end
    new_ends = map(x -> max(1, length(x)), ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length, fill_val, threads = threads)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing, threads = threads)
        end
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    true, dsl
end


function _join_inner_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check, threads = true) where T
    _fl = _date_value∘identity
    _fr = _date_value∘identity
    if mapformats[1]
        _fl = _date_value∘getformat(dsl, onleft[1])
    end
    if mapformats[2]
        _fr = _date_value∘getformat(dsr, onright[1])
    end

    dict, maxprob, sz, fallback, type = _create_dictionary_for_join(_fr, _columns(dsr)[onright[1]], _fl, _columns(dsl)[onleft[1]], Val(T))
    if fallback
        return false, Dataset()
    end
    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type, threads = threads)

    new_ends = map(length, ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end

    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            _fill_oncols_left_table_inner!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length, threads = threads)
        else
            _fill_oncols_left_table_inner!(_res, _columns(dsl)[j], ranges, new_ends, total_length, threads = threads)
        end
        push!(res, _res)
    end
    if dsl isa SubDataset
        newds = Dataset(res, copy(index(dsl)), copycols = false)
    else
        newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            _fill_right_cols_table_inner!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length, threads = threads)
        else
            _fill_right_cols_table_inner!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, threads = threads)
        end
        push!(_columns(newds), _res)

        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    true, newds

end

function _join_outer_dict(dsl, dsr, ranges, onleft, onright, oncols_left, oncols_right, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check, threads = true) where T
    _fl = _date_value∘identity
    _fr = _date_value∘identity
    if mapformats[1]
        _fl = _date_value∘getformat(dsl, onleft[1])
    end
    if mapformats[2]
        _fr = _date_value∘getformat(dsr, onright[1])
    end

    dict, maxprob, sz, fallback, type = _create_dictionary_for_join(_fr, _columns(dsr)[onright[1]], _fl, _columns(dsl)[onleft[1]], Val(T))
    if fallback
        return false, Dataset()
    end
    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type, threads = threads)
    new_ends = map(x -> max(1, length(x)), ranges)
    notinleft = _find_right_not_in_left(ranges, nrow(dsr), 1:nrow(dsr))
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end] + length(notinleft)
    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end
    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_oncols_left_table_left!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length, fill_val, threads = threads)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing, threads = threads)
        end
        push!(res, _res)
    end
    for j in 1:length(oncols_left)
        _fill_oncols_left_table_left_outer!(res[oncols_left[j]], _columns(dsr)[oncols_right[j]], notinleft, new_ends, total_length)
    end
    if dsl isa SubDataset
        newds = Dataset(res, copy(index(dsl)), copycols = false)
    else
        newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length, fill_val, threads = threads)
            _fill_oncols_left_table_left_outer!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), notinleft, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing, threads = threads)
            _fill_oncols_left_table_left_outer!(_res, _columns(dsr)[right_cols[j]], notinleft, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    true, newds

end

function _update!_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; allowmissing = true, mode = :all, mapformats = [true, true], stable = false, alg = HeapSort, threads = threads) where T
    _fl = _date_value∘identity
    _fr = _date_value∘identity
    if mapformats[1]
        _fl = _date_value∘getformat(dsl, onleft[1])
    end
    if mapformats[2]
        _fr = _date_value∘getformat(dsr, onright[1])
    end
    dict, maxprob, sz, fallback, type = _create_dictionary_for_join(_fr, _columns(dsr)[onright[1]], _fl, _columns(dsl)[onleft[1]], Val(T))
    # key is not unique, fall back to sort
    if fallback
        return false, Dataset()
    end

    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type, threads = threads)

    if mode == :all
        f_mode = x->true
    elseif mode == :missing || mode == :missings
        f_mode = x->ismissing(x)
    else
        f_mode = x->mode(x)::Bool
    end

    for j in 1:length(right_cols)
        if haskey(index(dsl).lookup, _names(dsr)[right_cols[j]])
            left_cols_idx = index(dsl)[_names(dsr)[right_cols[j]]]
            TL = nonmissingtype(eltype(_columns(dsl)[left_cols_idx]))
            TR = nonmissingtype(eltype(_columns(dsr)[right_cols[j]]))
            if promote_type(TR, TL) <: TL
                _update_left_with_right!(_columns(dsl)[left_cols_idx], _columns(dsr)[right_cols[j]], ranges, allowmissing, f_mode, threads = threads)
            end
        end
    end
    _modified(_attributes(dsl))
    true, dsl
end


# a new idea for joining without sorting
function _in_hash(dsl::AbstractDataset, dsr::AbstractDataset, ::Val{T}; onleft, onright, mapformats = [true, true], threads = true) where T
    isempty(dsl) && return Bool[]
    oncols_left = onleft
    oncols_right = onright

    # use Set when there is only one column in `on`
    cols = Any[]
    for j in 1:length(oncols_left)
        if mapformats[1]
            fl = getformat(dsl, oncols_left[j])
        else
            fl = identity
        end
        if mapformats[2]
            fr = getformat(dsr, oncols_right[j])
        else
            fr = identity
        end
        push!(cols, Cat2Vec(_columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], fl, fr))
    end
    newds = Dataset(cols, :auto, copycols = false)
    a = _gather_groups(newds, :, nrow(newds)< typemax(Int32) ? Val(Int32) : Val(Int64), stable = false, mapformats = false, threads = threads)
    res = _in_use_Set_int(view(a[1], 1:nrow(dsl)), view(a[1], nrow(dsl)+1:length(a[1])), 1, a[3]; threads = threads)
end

function _create_Set_for_join_int(f, v, minval, rangelen)
    flag = false
    offset = 1 - minval
    n = length(v)
    sz = rangelen + 1
    gslots = falses(sz)
    @inbounds for i in 1:length(v)
        _fv = f(v[i])
        if ismissing(_fv)
            slotix = sz
        else
            slotix = _fv + offset
        end
        if !gslots[slotix]
            gslots[slotix] = true
        end
    end
    gslots, minval, sz
end

function _query_Set_for_join_int(f, fv, gslots, minval, sz)
    offset = 1 - minval
    if ismissing(fv)
        slotix = sz
    else
        slotix = fv + offset
        !(slotix in 1:sz-1) && return 0
    end

    if slotix in 1:sz
        rowid = gslots[slotix]
        return rowid
    end
    false
end

function _in_use_Set_int_barrier!(res, ldata, gslots, minval, sz; threads = true)
    @_threadsfor threads for i in 1:length(res)
        res[i] = _query_Set_for_join_int(identity, ldata[i], gslots, minval, sz)
    end
end

function _in_use_Set_int(ldata, rdata, minval, rangelen; threads = true)
    gslots, minval, sz  =  _create_Set_for_join_int(identity, rdata, minval, rangelen)
    res = Vector{Bool}(undef, length(ldata))
    _in_use_Set_int_barrier!(res, ldata, gslots, minval, sz; threads = threads)
    res
end

# f is a function which should be applied on each element of v
# v is a vector of Int with minimum minval and range length of rangelen
# reps gives how many times a specific integer will appear in v
# no missing in v
function _create_dictionary_for_join_int(f, v, reps, minval, rangelen, ::Val{T}) where T
    offset = 1 - minval
    n = length(v)
    where = Vector{T}(undef, length(reps)+1)
    cumsum!(view(where, 2:length(where)), reps)
    where[1] = 0
    gslots = zeros(T, where[end])
    sz = rangelen
    @inbounds for i in 1:length(v)
        _fv = f(v[i])
        slotix = _fv + offset
        gslots[where[slotix]+1] = i
        where[slotix] += 1
    end
    gslots, minval, sz
end
# there is no missing in v
# here can be defined as:
# where = Vector{T}(undef, length(reps)+1)
# cumsum!(view(where, 2:length(where)), reps)
# where[1] = 0
function _query_dictionary_for_join_int(f, v, gslots,reps, where, minval, sz)
    offset = 1 - minval
    slotix = v + offset
    !(slotix in 1:sz) && return 1:0
    if slotix in 1:sz
        rowid = reps[slotix]
        rowid == 0 && return 1:0
        return where[slotix]+1:where[slotix+1]
    end
end




function _in_use_Set(ldata, rdata, _fl, _fr; threads = true)

    ss = Set(Base.Generator(_fr, rdata));
    res = Vector{Bool}(undef, length(ldata))
    @_threadsfor threads for i in 1:length(res)
        res[i] = _fl(ldata[i]) in ss
    end
    res
end
