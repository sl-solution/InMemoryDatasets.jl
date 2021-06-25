# first attemp for combine
# Don't use it yet
function combine(ds::Dataset, arg::Pair)
    !isgrouped(ds) && throw(ArgumentError("`combine` is only for grouped data sets"))
    idx_cpy = copy(index(ds))
    ms = normalize_modify!(idx_cpy, index(ds), arg)
    groupcols = index(ds).sortedcols
    starts = group_starts(ds)
    ngroups = index(ds).ngroups[]

    res = AbstractArray[]
    for j in 1:length(groupcols)
        push!(res, ds[starts, groupcols[j]])
    end
    T = _check_the_output_type(ds, ms)
    _res = Tables.allocatecolumn(T, ngroups)
    for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = ngroups : hi = starts[g + 1] - 1
        _res[g] = ms.second.first(view(_columns(ds)[ms.first], lo:hi))
    end
    push!(res, _res)
    nm = _names(ds)[groupcols]
    push!(nm, ms.second.second)
    Dataset(res, nm)
end
