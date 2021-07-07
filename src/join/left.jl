function _find_ranges_for_leftjoin!(ranges, x, y)
    Threads.@threads for i in 1:length(x)
        ranges[i] = searchsorted(y, x[i], ranges[i].start, ranges[i].stop, Base.Order.Forward)
    end
end
function _fill_oncols_left_table!(_res, x, ranges, en, total)
    Threads.@threads for i in 1:length(x)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        _res[lo:hi] .= x[i]
    end
end

function _fill_right_cols_left_table!(_res, x, ranges, en, total)
    Threads.@threads for i in 1:length(x)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        length(ranges[i]) == 0 ? _res[lo:hi] .= missing : _res[lo:hi] .= x[ranges[i]]
    end
end
function _left_join_main(dsl::Dataset, dsr::Dataset; on::MultiColumnIndex, makeunique = false, check = true)
    oncols_left = index(dsl)[on]
    oncols_right = index(dsr)[on]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    # dsr_oncols = select(dsr, oncols, copycols = true)
    sort!(dsr, oncols_right)
    ranges = Vector{UnitRange{Int}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _find_ranges_for_leftjoin!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]])
    end
    new_ends = map(x->max(1, length(x)), ranges)
    cumsum!(new_ends, new_ends)
    total_length = new_ends[end]
    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly"
    end
    res = []
    for j in 1:length(index(dsl))
        _res = Tables.allocatecolumn(eltype(_columns(dsl)[j]), total_length)
        _fill_oncols_left_table!(_res, _columns(dsl)[j], ranges, new_ends, total_length)
        push!(res, _res)
    end
    newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)

    for j in 1:length(right_cols)
        _res = Tables.allocatecolumn(Union{Missing, eltype(_columns(dsr)[right_cols[j]])}, total_length)
        _fill_right_cols_left_table!(_res, _columns(dsr)[right_cols[j]], ranges, new_ends, total_length)
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end

    newds

end
