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

function _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, x_l, x_r, sz, type)
    if _fr == _date_value∘identity && DataAPI.refpool(x_r) !== nothing
        invp = DataAPI.invrefpool(x_r)
        Threads.@threads for i in 1:length(x_l)
            revmap_paval_ref = get(invp, DataAPI.unwrap(_fl(x_l[i])), missing)
            loc = _query_dictionary_for_join_int(_fr, x_r,revmap_paval_ref, dict, maxprob, sz)
            if loc == 0
                ranges[i] = 1:0
            else
                ranges[i] = loc:loc
            end
        end
    elseif return_type(_fr, x_r) <: AbstractVector{<:Union{Missing, INTEGERS}} && type == 2
        Threads.@threads for i in 1:length(x_l)
            loc = _query_dictionary_for_join_int(_fr, x_r, _fl(x_l[i]), dict, maxprob, sz)
            if loc == 0
                ranges[i] = 1:0
            else
                ranges[i] = loc:loc
            end
        end
    elseif type == 1
        Threads.@threads for i in 1:length(x_l)
            loc = _query_dictionary_for_join_general(_fr, x_r, _fl(x_l[i]), dict, maxprob, sz)
            if loc == 0
                ranges[i] = 1:0
            else
                ranges[i] = loc:loc
            end
        end
    end
end


function _join_left_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check ) where T
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

    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type)

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
            _fill_oncols_left_table_left!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length, missing)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing)
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
            _fill_right_cols_table_left!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length, fill_val)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing)
        end
        push!(_columns(newds), _res)

        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    true, newds

end

function _join_left!_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check ) where T
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

    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type)

    new_ends = map(x -> max(1, length(x)), ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            # fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length, missing)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing)
        end
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    true, dsl
end


function _join_inner_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check) where T
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
    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type)

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
            _fill_oncols_left_table_inner!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length)
        else
            _fill_oncols_left_table_inner!(_res, _columns(dsl)[j], ranges, new_ends, total_length)
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
            _fill_right_cols_table_inner!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length)
        else
            _fill_right_cols_table_inner!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length)
        end
        push!(_columns(newds), _res)

        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    true, newds

end

function _join_outer_dict(dsl, dsr, ranges, onleft, onright, oncols_left, oncols_right, right_cols, ::Val{T}; makeunique = makeunique, mapformats = mapformats, check = check) where T
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
    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type)
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
            _fill_oncols_left_table_left!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length, fill_val)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing)
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
            _fill_right_cols_table_left!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), ranges, new_ends, total_length, fill_val)
            _fill_oncols_left_table_left_outer!(_res.refs, DataAPI.refarray(_columns(dsr)[right_cols[j]]), notinleft, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length, missing)
            _fill_oncols_left_table_left_outer!(_res, _columns(dsr)[right_cols[j]], notinleft, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    true, newds

end

function _in_use_Set(ldata, rdata, _fl, _fr)
    ss = Set(Base.Generator(_fr, rdata));
    res = Vector{Bool}(undef, length(ldata))
    Threads.@threads for i in 1:length(res)
        res[i] = _fl(ldata[i]) in ss
    end
    res
end


function _update!_dict(dsl, dsr, ranges, onleft, onright, right_cols, ::Val{T}; allowmissing = true, mode = :all, mapformats = [true, true], stable = false, alg = HeapSort) where T
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

    _fill_ranges_for_dict_join!(ranges, dict, maxprob, _fl, _fr, _columns(dsl)[onleft[1]], _columns(dsr)[onright[1]], sz, type)

    for j in 1:length(right_cols)
        if haskey(index(dsl).lookup, _names(dsr)[right_cols[j]])
            left_cols_idx = index(dsl)[_names(dsr)[right_cols[j]]]
            TL = nonmissingtype(eltype(_columns(dsl)[left_cols_idx]))
            TR = nonmissingtype(eltype(_columns(dsr)[right_cols[j]]))
            if promote_type(TR, TL) <: TL
                _update_left_with_right!(_columns(dsl)[left_cols_idx], _columns(dsr)[right_cols[j]], ranges, allowmissing, mode)
            end
        end
    end
    _modified(_attributes(dsl))
    true, dsl
end
