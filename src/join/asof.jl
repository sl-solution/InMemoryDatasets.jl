function _find_ranges_for_asofback!(ranges, x, y, _fl, _fr)
    Threads.@threads for i in 1:length(x)
        curr_start = ranges[i].start
        ranges[i] = searchsortedlast_join(_fr, y, _fl(x[i]), ranges[i].start, ranges[i].stop, Base.Order.Forward):1
        if ranges[i].start < curr_start
            ranges[i] = curr_start:0
        end
    end
end
function _find_ranges_for_asoffor!(ranges, x, y, _fl, _fr)
    Threads.@threads for i in 1:length(x)
        cur_stop = ranges[i].stop
        ranges[i] = searchsortedfirst_join(_fr, y, _fl(x[i]), ranges[i].start, ranges[i].stop, Base.Order.Forward):1
        if ranges[i].start > cur_stop
            ranges[i] = cur_stop:0
        end
    end
end

function  _fill_right_cols_table_asof!(_res, x, ranges, total, bordervalue)
    Threads.@threads for i in 1:length(ranges)
        _res[i] = x[ranges[i].start]
        if !bordervalue && ranges[i].stop == 0
            _res[i] = missing
        end
    end
end

# border = :nearest | :missing
function _join_asofback(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest) where T
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
    for j in 1:(length(oncols_left) - 1)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end

    _fl = getformat(dsl, oncols_left[length(oncols_left)])
    _fr = getformat(dsr, oncols_right[length(oncols_left)])
    _find_ranges_for_asofback!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)

    total_length = nrow(dsl)

    res = []
    for j in 1:length(index(dsl))
       push!(res,  _columns(dsl)[j])
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest)
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end

function _join_asofback!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest) where T
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
    for j in 1:(length(oncols_left) - 1)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end

    _fl = getformat(dsl, oncols_left[length(oncols_left)])
    _fr = getformat(dsr, oncols_right[length(oncols_left)])
    _find_ranges_for_asofback!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)

    total_length = nrow(dsl)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest)
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    dsl

end



function _join_asoffor(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest) where T
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
    for j in 1:(length(oncols_left) - 1)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end

    _fl = getformat(dsl, oncols_left[length(oncols_left)])
    _fr = getformat(dsr, oncols_right[length(oncols_left)])
    _find_ranges_for_asoffor!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)

    total_length = nrow(dsl)

    res = []
    for j in 1:length(index(dsl))
       push!(res,  _columns(dsl)[j])
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest)
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end
function _join_asoffor!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest) where T
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
    for j in 1:(length(oncols_left) - 1)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end

    _fl = getformat(dsl, oncols_left[length(oncols_left)])
    _fr = getformat(dsr, oncols_right[length(oncols_left)])
    _find_ranges_for_asoffor!(ranges, _columns(dsl)[oncols_left[length(oncols_left)]], _columns(dsr)[oncols_right[length(oncols_left)]], _fl, _fr)

    total_length = nrow(dsl)

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length, border == :nearest)
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    _modified(_attributes(dsl))
    dsl

end
