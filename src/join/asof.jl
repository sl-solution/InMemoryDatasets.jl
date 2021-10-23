function _find_ranges_for_asofback!(ranges, x, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        curr_start = ranges[i].start
        if length(ranges[i]) == 0
            ranges[i] = 0:0
        else
            ranges[i] = 1:searchsortedlast_join(_fr, y, _fl(x[i])::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            if ranges[i].stop < curr_start
                ranges[i] = 0:curr_start
            end
        end
    end
end
function _find_ranges_for_asoffor!(ranges, x, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        cur_stop = ranges[i].stop
        if length(ranges[i]) == 0
            ranges[i] = 0:0
        else
            ranges[i] = 1:searchsortedfirst_join(_fr, y, _fl(x[i])::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            if ranges[i].stop > cur_stop
                ranges[i] = 0:cur_stop
            end
        end
    end
end

function  _fill_right_cols_table_asof!(_res, x, ranges, total, bordervalue, fill_val)
    Threads.@threads for i in 1:length(ranges)
        if ranges[i] == 0:0
            _res[i] = missing
        else
            _res[i] = x[ranges[i].stop]
            if !bordervalue && ranges[i].start == 0
                _res[i] = missing
            end
        end
    end
end

function _change_refpool_find_range_for_asof!(ranges, dsl, dsr, oncols_left, oncols_right, direction, lmf, rmf, j)
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
        if direction == :backward
            _find_ranges_for_asofback!(ranges, var_l.refs, var_r.refs, _fl, _fr, Val(T1), Val(T2))
        elseif direction == :forward
            _find_ranges_for_asoffor!(ranges, var_l.refs, var_r.refs, _fl, _fr, Val(T1), Val(T2))
        end
    elseif DataAPI.refpool(var_l) !== nothing && !(var_l isa PooledArray)
        dict_l = _generate_inverted_dict_pool(var_l)
        T1 = Core.Compiler.return_type(format_l, (valtype(dict_l), ))
        T2 = Core.Compiler.return_type(format_r, (eltype(var_r), ))
        _fl = x -> format_l(dict_l[x])
        _fr = format_r
        if direction == :backward
            _find_ranges_for_asofback!(ranges, var_l.refs, var_r, _fl, _fr, Val(T1), Val(T2))
        elseif direction == :forward
            _find_ranges_for_asoffor!(ranges, var_l.refs, var_r, _fl, _fr, Val(T1), Val(T2))
        end
    elseif DataAPI.refpool(var_r) !== nothing && !(var_r isa PooledArray)
        dict_r = _generate_inverted_dict_pool(var_r)
        T2 = Core.Compiler.return_type(format_r, (valtype(dict_r), ))
        T1 = Core.Compiler.return_type(format_l, (eltype(var_l), ))
        _fl = format_l
        _fr = x -> format_r(dict_r[x])
        if direction == :backward
            _find_ranges_for_asofback!(ranges, var_l, var_r.refs, _fl, _fr, Val(T1), Val(T2))
        elseif direction == :forward
            _find_ranges_for_asoffor!(ranges, var_l, var_r.refs, _fl, _fr, Val(T1), Val(T2))
        end
    else
        T1 = Core.Compiler.return_type(format_l, (eltype(var_l), ))
        T2 = Core.Compiler.return_type(format_r, (eltype(var_r), ))
        _fl = format_l
        _fr = format_r
        if direction == :backward
            _find_ranges_for_asofback!(ranges, var_l, var_r, _fl, _fr, Val(T1), Val(T2))
        elseif direction == :forward
            _find_ranges_for_asoffor!(ranges, var_l, var_r, _fl, _fr, Val(T1), Val(T2))
        end
    end
end



# border = :nearest | :missing
function _join_asofback(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest, mapformats = [true, true], stable = false, alg = HeapSort) where T
    isempty(dsl) && return copy(dsl)
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right, stable = stable, alg = alg)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:(length(oncols_left) - 1)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end

    # _fl = getformat(dsl, oncols_left[length(oncols_left)])
    # _fr = getformat(dsr, oncols_right[length(oncols_left)])
    # _find_ranges_for_asofback!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)
    _change_refpool_find_range_for_asof!(ranges, dsl, dsr, oncols_left, oncols_right, :backward, mapformats[1], mapformats[2], length(oncols_left))
    total_length = nrow(dsl)

    res = []
    for j in 1:length(index(dsl))
       push!(res,  _columns(dsl)[j])
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_asof!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, total_length, border == :nearest, fill_val)
        else
            _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest, missing)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end

function _join_asofback!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest, mapformats = [true, true], stable =false, alg = HeapSort) where T
    isempty(dsl) && return dsl
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right, stable = stable, alg = alg)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:(length(oncols_left) - 1)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end

    # _fl = getformat(dsl, oncols_left[length(oncols_left)])
    # _fr = getformat(dsr, oncols_right[length(oncols_left)])
    # _find_ranges_for_asofback!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)
    _change_refpool_find_range_for_asof!(ranges, dsl, dsr, oncols_left, oncols_right, :backward, mapformats[1], mapformats[2], length(oncols_left))


    total_length = nrow(dsl)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_asof!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, total_length, border == :nearest, fill_val)
        else
            _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest, missing)
        end
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    dsl

end



function _join_asoffor(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest, mapformats = [true, true], stable = false, alg = HeapSort) where T
    isempty(dsl) && return copy(dsl)
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right, stable = stable, alg = alg)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:(length(oncols_left) - 1)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end

    # _fl = getformat(dsl, oncols_left[length(oncols_left)])
    # _fr = getformat(dsr, oncols_right[length(oncols_left)])
    # _find_ranges_for_asoffor!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)
    _change_refpool_find_range_for_asof!(ranges, dsl, dsr, oncols_left, oncols_right, :forward, mapformats[1], mapformats[2], length(oncols_left))


    total_length = nrow(dsl)

    res = []
    for j in 1:length(index(dsl))
       push!(res,  _columns(dsl)[j])
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_asof!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, total_length, border == :nearest, fill_val)
        else
            _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest, missing)
        end
        # _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest)
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end
function _join_asoffor!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest, mapformats = [true, true], stable = false, alg = HeapSort) where T
    isempty(dsl) && return dsl
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right, stable = stable, alg = alg)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:(length(oncols_left) - 1)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end

    # _fl = getformat(dsl, oncols_left[length(oncols_left)])
    # _fr = getformat(dsr, oncols_right[length(oncols_left)])
    # _find_ranges_for_asoffor!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)
    _change_refpool_find_range_for_asof!(ranges, dsl, dsr, oncols_left, oncols_right, :forward, mapformats[1], mapformats[2], length(oncols_left))


    total_length = nrow(dsl)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_asof!(_res.refs, _columns(dsr)[right_cols[j]].refs, ranges, total_length, border == :nearest, fill_val)
        else
            _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest, missing)
        end
        # _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest)
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    dsl

end
