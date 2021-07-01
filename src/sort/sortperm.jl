function _sortperm_unstable!(idx, x, ranges, last_valid_range, ord)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        df_sort!(x, idx, rangestart, rangeend, QuickSort, ord)
    end
end


function _sortperm_pooledarray!(idx, idx_cpy, x, xpool, where, counts, ranges, last_valid_range, ord)
    ngroups = length(xpool)
    perm = sortperm(xpool, QuickSort, ord)
    iperm = invperm(perm)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        _group_indexer!(x::Vector, original_P, copy_P, where[Threads.threadid()], counts[Threads.threadid()], lo, hi, ngroups::Integer, perm, iperm)
    end
end


# dates should be treated as integer
_date_value(x::TimeType) = Dates.value(x)
_date_value(x::Period) = Dates.value(x)
_date_value(x) = x
