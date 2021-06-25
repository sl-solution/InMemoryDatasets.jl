# first attemp for combine
# Don't use it yet
function combine(ds::Dataset, @nospecialize(args...))
    !isgrouped(ds) && throw(ArgumentError("`combine` is only for grouped data sets"))
    idx_cpy = Index(copy(index(ds).lookup), copy(index(ds).names), Dict{Int, Function}())
    ms = normalize_modify_multiple!(idx_cpy, index(ds), args...)
    _first_vector_res = findfirst(x->!(x.second.first isa Expr) && 
        (return_type(x.second.first, (typeof(ds[!, x.first].val),)) <: AbstractVector), ms)


    any(x->x.second.first isa Expr, ms) && throw(ArgumentError("`byrow` is not available for `combine`"))
    groupcols = index(ds).sortedcols
    starts = index(ds).starts
    ngroups = index(ds).ngroups[]

    # TODO should we allocate the result and reuse it later?
    # here we don't want to allocate, but it means we should compute
    # the transformation twice
    if _first_vector_res !== nothing
        new_ds = _combine(ds, ms, _first_vector_res)
        return new_ds     
    end

    
    res = AbstractArray[]
    for j in 1:length(groupcols)
        push!(res, _columns(ds)[groupcols[j]][view(starts, 1:ngroups)])
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
    Dataset(res, nm, copycols = false)
end

function _combine(ds::Dataset, ms, _first_vector_res)
    
    groupcols = index(ds).sortedcols
    starts = index(ds).starts
    ngroups = index(ds).ngroups[]

    # TODO should we allocate the result and reuse it later?
    # here we don't want to allocate, but it means we should compute
    # the transformation twice
   
    _first_vector_var = ms[_first_vector_res].second.second
    CT = return_type(ms[_first_vector_res].second.first,
             (typeof(ds[!, ms[_first_vector_res].first].val),))
    special_res = Vector{CT}(undef, ngroups)
    new_lengths = Vector{Int}(undef, ngroups)
    Threads.@threads for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = nrow(ds) : hi = starts[g + 1] - 1
        special_res[g] = ms[_first_vector_res].second.first(view(_columns(ds)[ms[_first_vector_res].first], lo:hi))
        new_lengths[g] = length(special_res[g])
    end
    total_lengths = sum(new_lengths)
    

    
    res = AbstractArray[]
    for j in 1:length(groupcols)
        _tmpres = similar(ds[!, groupcols[j]], total_lengths)
        Threads.@threads for i in 1:ngroups
            for k in 1:new_lengths[i]
                _tmpres[(i-1)*new_lengths[i] + k] = _columns(ds)[groupcols[j]][starts[i]]
            end
        end
        push!(res, _tmpres)
    end
    nm = _names(ds)[groupcols]
    for i in 1:length(ms)
        T = _check_the_output_type(ds, ms[i])
        _res = Tables.allocatecolumn(T, total_lengths)
        if i == _first_vector_res
            Threads.@threads for i1 in 1:ngroups
                for k in 1:new_lengths[i1]
                    _res[(i1-1)*new_lengths[i1] + k] = special_res[i1][k]
                end
            end
            push!(res, _res)
            push!(nm, ms[i].second.second)
            continue
        end
        # make sure lo and hi are not defined any where outside the following loops
        Threads.@threads for g in 1:ngroups
            lo = starts[g]
            g == ngroups ? hi = nrow(ds) : hi = starts[g + 1] - 1
            _res[(g-1)*new_lengths[g]+1:g*new_lengths[g]] .= ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
            # _res[g] = ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
         end
        push!(res, _res)
        push!(nm, ms[i].second.second)
    end
    Dataset(res, nm, copycols = false)
end
