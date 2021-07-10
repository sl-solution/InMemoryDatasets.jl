function _find_ranges_for_asofback!(ranges, x, y, _fl, _fr)
    Threads.@threads for i in 1:length(x)
        ranges[i] = searchsortedlast_join(_fr, y, _fl(x[i]), 1, length(y), Base.Order.Forward)
        if ranges[i] == 0
            ranges[i] = 1
        end
    end
end
function _find_ranges_for_asoffor!(ranges, x, y)
    Threads.@threads for i in 1:length(x)
        ranges[i] = searchsortedfirst_join(_fr, y, _fl(x[i]), 1, length(y), Base.Order.Forward)
        if ranges[i] > length(y)
            ranges[i] = length(y)
        end
    end
end

function  _fill_right_cols_table_asof!(_res, x, ranges, total)
    Threads.@threads for i in 1:length(ranges)
        _res[i] = x[ranges[i]]
    end
end

function _join_asofback(dsl::Dataset, dsr::Dataset; onleft, onright, makeunique = false)
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl)[oncols_left], _names(dsr)[oncols_right]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right)
    ranges = Vector{Int}(undef, nrow(dsl))

    _fl = getformat(dsl, oncols_left[1])
    _fr = getformat(dsr, oncols_right[1])
    _find_ranges_for_asofback!(ranges, _columns(dsl)[oncols_left[1]], _columns(dsr)[oncols_right[1]], _fl, _fr)

    total_length = nrow(dsl)

    res = []
    for j in 1:length(index(dsl))
       push!(res,  _columns(dsl)[j])
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = Tables.allocatecolumn(eltype(_columns(dsr)[right_cols[j]]), total_length)
        _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length)
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end


function _join_asoffor(dsl::Dataset, dsr::Dataset; onleft, onright, makeunique = false)
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl)[oncols_left], _names(dsr)[oncols_right]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right)
    ranges = Vector{Int}(undef, nrow(dsl))

    _fl = getformat(dsl, oncols_left[1])
    _fr = getformat(dsr, oncols_right[1])
    _find_ranges_for_asoffor!(ranges, _columns(dsl)[oncols_left[1]], _columns(dsr)[oncols_right[1]], _fl, _fr)

    total_length = nrow(dsl)

    res = []
    for j in 1:length(index(dsl))
       push!(res,  _columns(dsl)[j])
    end

    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = Tables.allocatecolumn(eltype(_columns(dsr)[right_cols[j]]), total_length)
        _fill_right_cols_table_asof!(_res, _columns(dsr)[right_cols[j]], ranges, total_length)
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    newds

end