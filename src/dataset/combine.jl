function _is_byrow_valid(idx, ms)
    righthands = Int[]
    lookupdict = idx.lookup
    if ms[1].second.first isa Expr
        return false
    end
    for i in 1:length(ms)
        if (ms[i].second.first isa Expr) && ms[i].second.first.head == :BYROW
            
            byrow_vars = idx[ms[i].first]
            !all(byrow_vars .∈ Ref(righthands)) && return false
        end
        if haskey(idx, ms[i].second.second)
            push!(righthands, idx[ms[i].second.second])
        end
    end
    return true
end

function _check_mutliple_rows_for_each_group(ds, ms)
    for i in 1:length(ms)
        # byrow are not checked since they are not going to modify the number of rows
        if !(ms[i].second.first isa Expr) &&
                 haskey(index(ds), ms[i].first) && 
                    !(ms[i].first ∈ map(x->x.second.second, view(ms, 1:(i-1))))
            T = return_type(ms[i].second.first, (typeof(ds[!, ms[i].first].val),))
            if T <: AbstractVector
                return i
            end
        end
    end
    return 0
end

function _is_groupingcols_modifed(ds, ms)
    groupcols::Vector{Int} = index(ds).sortedcols
    idx = index(ds)
    all_names = _names(ds)
    for i in 1:length(ms)
        if (ms[i].second.second ∈ all_names) && (idx[ms[i].second.second] ∈ groupcols)
            return true
        end
    end
    return false
end

function _compute_the_mutli_row_trans!(special_res, new_lengths, x, nrows, _f, _first_vector_res, starts, ngroups)
    # _first_vector_var = ms[_first_vector_res].second.second
    Threads.@threads for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        special_res[g] = _f(view(x, lo:hi))
        new_lengths[g] = length(special_res[g])
    end
end

# this returns lookup dictionary and names for the new ds
function _create_index_for_newds(ds, ms)
    all_names = _names(ds)
    nm = Symbol[]
    lookup = Dict{Symbol, Int}()
    cnt = 1
    for j in 1:length(index(ds).sortedcols)
        new_name = all_names[index(ds).sortedcols[j]]
        if !haskey(lookup, new_name)
            push!(nm, new_name)
            push!(lookup, new_name => cnt)
            cnt += 1
        end
    end
    for i in 1:length(ms)
        new_name = ms[i].second.second
         if !haskey(lookup, new_name)
            push!(nm, new_name)
            push!(lookup, new_name => cnt)
            cnt += 1
        end
    end
    return (lookup, nm)
end


function _push_groups_to_res!(res, _tmpres, x, ds, starts, new_lengths, total_lengths, j)
    groupcols = index(ds).sortedcols
    ngroups::Int = index(ds).ngroups[]
    counter::UnitRange{Int} = 1:1
    for i in 1:ngroups
        i == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[i - 1] + 1):new_lengths[i])
        for k in 1:length(counter)
            _tmpres[new_lengths[i] - length(counter) + k] = x[starts[i]]
        end
    end
    push!(res, _tmpres)
    return _tmpres
end


function _check_the_output_type(x, mssecond)
    CT = return_type(mssecond, (typeof(x),))
    # TODO check other possibilities:
    # the result can be
    # * AbstractVector{T} where T
    # * Vector{T}
    # * not a Vector
    CT == Union{} && throw(ArgumentError("compiler cannot assess the return type of calling `$(mssecond)` on input, you may want to try using `byrow`"))
    if CT <: AbstractVector
        if hasproperty(CT, :var)
            T = CT.var.ub
        else
            T = eltype(CT)
        end
    else
        T = CT
    end
    return T
end

function _update_one_col_combine!(res, _res, x, _f, ngroups, new_lengths, total_lengths, col)
    # make sure lo and hi are not defined any where outside the following loop
    Threads.@threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = new_lengths[g] - length(counter) + 1
        hi = new_lengths[g]
        _res[lo:hi] .= _f(view(x, counter))
        # _res[g] = ms.second.first(view(_columns(ds)[ms[i].first], lo:hi))
    end
    res[col] = _res
    return _res
end

function _add_one_col_combine_from_combine!(res, _res, x, _f, ngroups, new_lengths, total_lengths)
    # make sure lo and hi are not defined any where outside the following loop
    Threads.@threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = new_lengths[g] - length(counter) + 1
        hi = new_lengths[g]
        _res[lo:hi] .= _f(view(x, counter))
        # _res[g] = ms.second.first(view(_columns(ds)[ms[i].first], lo:hi))
    end
    push!(res, _res)
    return _res
end


function _add_one_col_combine!(res, _res, in_x, ds, _f, starts, ngroups, new_lengths, total_lengths)
    # make sure lo and hi are not defined any where outside the following loop
    Threads.@threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = starts[g]
        g == ngroups ? hi = nrow(ds) : hi = starts[g + 1] - 1
        l1 = new_lengths[g] - length(counter) + 1
        h1 = new_lengths[g]
        _res[l1:h1] .= _f(view(in_x, lo:hi))
        # _res[g] = ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
    end
    push!(res, _res)
    return _res
end

# special_res cannot be based on previous columns of the combined data set
function _fill_res_with_special_res!(res, _res, special_res, ngroups, new_lengths, total_lengths)
     Threads.@threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        for k in 1:length(counter)
            _res[new_lengths[g] - length(counter) + k] = special_res[g][k]
        end
    end
    push!(res, _res)
end
# special_res cannot be based on previous columns of the combined data set
function _update_res_with_special_res!(res, _res, special_res, ngroups, new_lengths, total_lengths, col)
     Threads.@threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        for k in 1:length(counter)
            _res[new_lengths[g] - length(counter) + k] = special_res[g][k]
        end
    end
    res[col] = _res
    return _res
end

function _combine_f_barrier_special(special_res, ds, newds, msfirst, mssecond, mslast, newds_lookup, _first_vector_res, ngroups, new_lengths, total_lengths)
    if !haskey(newds_lookup, mslast) #&& newlookup[all_names[ms[i].first]] != newlookup[ms[i].second.second]
        T = _check_the_output_type(ds[!, msfirst].val, mssecond)
        _res = Tables.allocatecolumn(T, total_lengths)
        _fill_res_with_special_res!(_columns(newds), _res, special_res, ngroups, new_lengths, total_lengths)
    else
        # update the existing column in newds
        T = _check_the_output_type(ds[!, msfirst].val, mssecond)
        _res = Tables.allocatecolumn(T, total_lengths)
        _update_res_with_special_res!(_columns(newds), _res, special_res, ngroups, new_lengths, total_lengths, newds_lookup[mslast])
    end
end


function _combine_f_barrier(ds, newds, msfirst, mssecond, mslast, newds_lookup, starts, ngroups, new_lengths, total_lengths)
    if !(mssecond isa Expr) && haskey(newds_lookup, msfirst)
        if !haskey(newds_lookup, mslast)
            T = _check_the_output_type(_columns(newds)[newds_lookup[msfirst]], mssecond)
            _res = Tables.allocatecolumn(T, total_lengths)
            _add_one_col_combine_from_combine!(_columns(newds), _res, _columns(newds)[newds_lookup[msfirst]], mssecond, ngroups, new_lengths, total_lengths)
        else
            T = _check_the_output_type(_columns(newds)[newds_lookup[msfirst]], mssecond)
            _res = Tables.allocatecolumn(T, total_lengths)
            _update_one_col_combine!(_columns(newds), _res, _columns(newds)[newds_lookup[msfirst]], mssecond, ngroups, new_lengths, total_lengths, newds_lookup[mslast])
        end
    elseif !(mssecond isa Expr) && !haskey(newds_lookup, msfirst)
        if !haskey(newds_lookup, mslast)
            T = _check_the_output_type(ds[!, msfirst].val, mssecond)
            _res = Tables.allocatecolumn(T, total_lengths)
            _add_one_col_combine!(_columns(newds), _res, _columns(ds)[index(ds)[msfirst]], ds, mssecond, starts, ngroups, new_lengths, total_lengths)
        else
            T = _check_the_output_type(ds[!, msfirst].val, mssecond)
            _res = Tables.allocatecolumn(T, total_lengths)
            _update_one_col_combine!(_columns(newds), _res, _columns(ds)[index(ds)[msfirst]], mssecond, ngroups, new_lengths, total_lengths, newds_lookup[mslast])
        end
    elseif (mssecond isa Expr) && mssecond.head == :BYROW
        push!(_columns(newds), byrow(newds, mssecond.args[1], msfirst; mssecond.args[2]...))
    else
        throw(ArgumentError("`combine` doesn't support $(msfirst=>mssecond=>mslast) combination"))
    end
end

# first attemp for combine
# Don't use it yet
function combine(ds::Dataset, @nospecialize(args...))
    !isgrouped(ds) && throw(ArgumentError("`combine` is only for grouped data sets, use `modify` instead"))
    idx_cpy::Index = Index(copy(index(ds).lookup), copy(index(ds).names), Dict{Int, Function}())
    ms = normalize_modify_multiple!(idx_cpy, index(ds), args...)
    # the rule is that in combine, byrow must only be used for already aggregated columns
    # so, we should check every thing pass to byrow has been assigned in args before it
    # if this is not the case, throw ArgumentError and ask user to use modify instead
    newlookup, new_nm = _create_index_for_newds(ds, ms)
    !(_is_byrow_valid(Index(newlookup, new_nm, Dict{Int, Function}()), ms)) && throw(ArgumentError("`byrow` must be used for aggregated columns, use `modify` otherwise"))
    # _check_mutliple_rows_for_each_group return the first transformation which causes multiple
    # rows or 0 if all transformations return scalar for each group
    # the transformation returning multiple rows must not be based on the previous columns in combine
    # result (which seems reasonable ??)
    _first_vector_res = _check_mutliple_rows_for_each_group(ds, ms)

    _is_groupingcols_modifed(ds, ms) && throw(ArgumentError("`combine` cannot modify the grouping columns"))

    groupcols::Vector{Int} = index(ds).sortedcols
    starts::Vector{Int} = index(ds).starts
    ngroups::Int = index(ds).ngroups[]

    # TODO should we allocate the result and reuse it later?
    # here we don't want to allocate, but it means we should compute
    # the transformation twice
    # we will use new_lengths later for assigning the grouping info of the new ds
    if _first_vector_res == 0
        new_lengths = ones(Int, ngroups)
        cumsum!(new_lengths, new_lengths)
        total_lengths = ngroups
    else
        CT = return_type(ms[_first_vector_res].second.first,
                 (typeof(ds[!, ms[_first_vector_res].first].val),))
        special_res = Vector{CT}(undef, ngroups)
        new_lengths = Vector{Int}(undef, ngroups)
        # _columns(ds)[ms[_first_vector_res].first]
        _compute_the_mutli_row_trans!(special_res, new_lengths, _columns(ds)[index(ds)[ms[_first_vector_res].first]], nrow(ds), ms[_first_vector_res].second.first, _first_vector_res, starts, ngroups)
        # special_res, new_lengths = _compute_the_mutli_row_trans(ds, ms, _first_vector_res, starts, ngroups)
        cumsum!(new_lengths, new_lengths)
        total_lengths = new_lengths[end]
    end
    all_names = _names(ds)

    # this is the columns for the output ds
    # this make sure that we will reuse new_lengths
    newds_idx = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}(), Int[], Bool[], false, [], Int[], 1)

    newds = Dataset([], newds_idx)
    newds_lookup = index(newds).lookup
    var_cnt = 1
    for j in 1:length(groupcols)
        _push_groups_to_res!(_columns(newds), similar(ds[!, groupcols[j]].val, total_lengths), _columns(ds)[groupcols[j]], ds, starts, new_lengths, total_lengths, j)
        push!(index(newds), new_nm[var_cnt])
        setformat!(newds, new_nm[var_cnt] => get(index(ds).format, groupcols[j], identity))
        var_cnt += 1

    end
    for i in 1:length(ms)
        # if newlookup has the name and has been created then use it otherwise use the
        # values from input data set

        if i == _first_vector_res
            _combine_f_barrier_special(special_res, ds, newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, _first_vector_res,ngroups, new_lengths, total_lengths)
            # if !haskey(newds_lookup, newlookup[all_names[ms[i].first]]) #&& newlookup[all_names[ms[i].first]] != newlookup[ms[i].second.second]
            #     T = _check_the_output_type(ds, ms[i])
            #     _res = Tables.allocatecolumn(T, total_lengths)
            #     _fill_res_with_special_res!(_columns(newds), _res, special_res, ngroups, new_lengths, total_lengths)
            # else
            #     # update the existing column in newds
            #     T = _check_the_output_type(ds, ms[i])
            #     _res = Tables.allocatecolumn(T, total_lengths)
            #     _update_res_with_special_res!(_columns(newds), _res, special_res, ngroups, new_lengths, total_lengths, newlookup[all_names[ms[i].first]])
            # end
        else
            _combine_f_barrier(ds, newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, starts, ngroups, new_lengths, total_lengths)
        end
        if !haskey(index(newds), ms[i].second.second)
            push!(index(newds), ms[i].second.second)
        end

    end
    # newds_index = Index(newlookup, new_nm, Dict{Int, Function}(), copy(index(ds).sortedcols),
    #     copy(index(ds).rev), true, [],[], ngroups)
    # newds = Dataset(res, new_nm)
    # grouping information for the output dataset
    append!(index(newds).sortedcols, index(ds).sortedcols)
    append!(index(newds).rev, index(ds).rev)
    append!(index(newds).perm, collect(1:total_lengths))
    index(newds).grouped[] = true
    index(newds).ngroups[] = ngroups
    append!(index(newds).starts, collect(1:total_lengths))
    for i in 2:(length(new_lengths))
        index(newds).starts[i] = new_lengths[i - 1]+1
    end
    newds
end
