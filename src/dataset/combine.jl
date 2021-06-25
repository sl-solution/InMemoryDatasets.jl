# first attemp for combine
# Don't use it yet
function combine(ds::Dataset, @nospecialize(args...); makeunique = false)
    !isgrouped(ds) && throw(ArgumentError("`combine` is only for grouped data sets"))
    idx_cpy = Index(copy(index(ds).lookup), copy(index(ds).names), copy(index(ds).format))
    ms = normalize_modify_multiple!(idx_cpy, index(ds), args...)
    any(x->x.second.first isa Expr, ms) && throw(ArgumentError("`byrow` is not available for `combine`"))
    groupcols = index(ds).sortedcols
    starts = index(ds).starts
    ngroups = index(ds).ngroups[]

    res = AbstractArray[]
    for j in 1:length(groupcols)
        _tmpres = similar(ds[!, groupcols[j]], ngroups)
        for i in 1:ngroups
            _tmpres[i] = _columns(ds)[groupcols[j]][starts[i]]
        end
        push!(res, _tmpres)
    end
    nm = _names(ds)[groupcols]
    for i in 1:length(ms)
        T = _check_the_output_type(ds, ms[i])
        _res = Tables.allocatecolumn(T, ngroups)
        # make sure lo and hi are not defined any where outside the following loops
        Threads.@threads for g in 1:ngroups
            lo = starts[g]
            g == ngroups ? hi = nrow(ds) : hi = starts[g + 1] - 1
            _res[g] = ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
         end
        push!(res, _res)
        push!(nm, ms[i].second.second)
    end
    Dataset(res, nm, copycols = false; makeunique = makeunique)
end
