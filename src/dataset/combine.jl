function _is_byrow_valid(idx_cpy, ms)
    righthands = Int[]
    dslookup = idx_cpy.lookup
    if ms[1].second.first isa Expr
        return false
    end
    for i in 1:length(ms)
        if (ms[i].second.first isa Expr) && ms[i].second.first.head == :BYROW
            byrow_vars = idx_cpy[ms[i].first]
            !all(byrow_vars .∈ Ref(righthands)) && return false
        end
        if haskey(dslookup, ms[i].second.second)
            push!(righthands, dslookup[ms[i].second.second])
        end
    end
    return true
end

function _check_mutliple_rows_for_each_group(ds, ms)
    for i in 1:length(ms)
        # byrow are not checked since they are going to modify the number of rows
        if !(ms[i].second.first isa Expr) && ms[i].first <= length(index(ds))
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

function _compute_the_mutli_row_trans(ds, ms, _first_vector_res, starts, ngroups)
    # _first_vector_var = ms[_first_vector_res].second.second
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
    (special_res, new_lengths)
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


function _push_groups_to_res!(res, ds, starts, new_lengths, total_lengths, j)
    groupcols = index(ds).sortedcols
    ngroups = index(ds).ngroups[]
    _tmpres = similar(ds[!, groupcols[j]], total_lengths)
    for i in 1:ngroups
        i == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[i - 1] + 1):new_lengths[i])
        for k in 1:length(counter)
            _tmpres[(i-1)*length(counter) + k] = _columns(ds)[groupcols[j]][starts[i]]
        end
    end
    push!(res, _tmpres)
    return _tmpres
end


function _check_the_output_type(x, ms)::DataType
    CT = return_type(ms.second.first, (typeof(x),))
    # TODO check other possibilities:
    # the result can be
    # * AbstractVector{T} where T
    # * Vector{T}
    # * not a Vector
    CT == Union{} && throw(ArgumentError("compiler cannot assess the return type of calling `$(ms.second.first)` on input, you may want to try using `byrow`"))
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

function _update_one_col_combine!(res, x, ms, ngroups, new_lengths, total_lengths, col, ::Val{T}) where T
    _res = Tables.allocatecolumn(T, total_lengths)
    # make sure lo and hi are not defined any where outside the following loop
    Threads.@threads for g in 1:ngroups
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        _res[(g-1)*length(counter)+1:g*length(counter)] .= ms.second.first(view(x, counter))
        # _res[g] = ms.second.first(view(_columns(ds)[ms[i].first], lo:hi))
    end
    res[col] = _res
    return _res
end

function _add_one_col_combine!(res, ds, ms, starts, ngroups, new_lengths, total_lengths, ::Val{T}) where T
    _res = Tables.allocatecolumn(T, total_lengths)
    # make sure lo and hi are not defined any where outside the following loop
    Threads.@threads for g in 1:ngroups
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = starts[g]
        g == ngroups ? hi = nrow(ds) : hi = starts[g + 1] - 1
        _res[(g-1)*length(counter)+1:g*length(counter)] .= ms.second.first(view(_columns(ds)[ms.first], lo:hi))
        # _res[g] = ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
    end
    push!(res, _res)
    return _res
end

# special_res cannot be based on previous columns of the combined data set
function _fill_res_with_special_res!(res, special_res, ngroups, new_lengths, total_lengths, ::Val{T}) where T
     _res = Tables.allocatecolumn(T, total_lengths)
     Threads.@threads for g in 1:ngroups
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        for k in 1:length(counter)
            _res[(g-1)*length(counter) + k] = special_res[g][k]
        end
    end
    push!(res, _res)
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
    !(_is_byrow_valid(idx_cpy, ms)) && throw(ArgumentError("`byrow` must be used for aggregated columns, use `modify` instead"))

    # _check_mutliple_rows_for_each_group return the first transformation which causes multiple
    # rows or 0 if all transformations return scalar for each group
    # the transformation returning multiple rows must not be based on the previous columns in combine
    # result (which seems reasonable ??)
    _first_vector_res = _check_mutliple_rows_for_each_group(ds, ms, )

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
        special_res, new_lengths = _compute_the_mutli_row_trans(ds, ms, _first_vector_res, starts, ngroups)
        cumsum!(new_lengths, new_lengths)
        total_lengths = new_lengths[end]
    end
    newlookup, new_nm = _create_index_for_newds(ds, ms)

    all_names = _names(ds)

    # this is the columns for the output ds
    res = AbstractArray[]
    
    for j in 1:length(groupcols)
        _push_groups_to_res!(res, ds, starts, new_lengths, total_lengths, j)
    end
    for i in 1:length(ms)
        # if newlookup has the name and has been created then use it otherwise use the
        # values from input data set
    
        if i == _first_vector_res
            T = _check_the_output_type(ds, ms[i])
            _fill_res_with_special_res!(res, special_res, ngroups, new_lengths, total_lengths, Val(T))
        elseif !(ms[i].second.first isa Expr) && haskey(newlookup, all_names[ms[i].first])
            if length(res) >= newlookup[all_names[ms[i].first]]
                T = _check_the_output_type(res[newlookup[all_names[ms[i].first]]], ms[i]) 
                _update_one_col_combine!(res, res[newlookup[all_names[ms[i].first]]], ms[i], ngroups, new_lengths, total_lengths, newlookup[all_names[ms[i].first]], Val(T))
            else
                T = _check_the_output_type(ds, ms[i])
                 _add_one_col_combine!(res, ds, ms[i], starts, ngroups, new_lengths, total_lengths, Val(T))
            end
        elseif !(ms[i].second.first isa Expr) && !haskey(newlookup, all_names[ms[i].first])
            T = _check_the_output_type(ds, ms[i])
            _add_one_col_combine!(res, ds, ms[i], starts, ngroups, new_lengths, total_lengths, Val(T))
        elseif (ms[i].second.first isa Expr) && ms[i].second.first.head == :BYROW
            @error "not yet implemented"
        end
    end
    # newds_index = Index(newlookup, new_nm, Dict{Int, Function}(), copy(index(ds).sortedcols),
    #     copy(index(ds).rev), true, [],[], ngroups)
    newds = Dataset(res, new_nm)
    newds
end

# function _combine(ds::Dataset, ms, _first_vector_res)
    
#     groupcols = index(ds).sortedcols
#     starts = index(ds).starts
#     ngroups = index(ds).ngroups[]

#     # TODO should we allocate the result and reuse it later?
#     # here we don't want to allocate, but it means we should compute
#     # the transformation twice
   
#     _first_vector_var = ms[_first_vector_res].second.second
#     CT = return_type(ms[_first_vector_res].second.first,
#              (typeof(ds[!, ms[_first_vector_res].first].val),))
#     special_res = Vector{CT}(undef, ngroups)
#     new_lengths = Vector{Int}(undef, ngroups)
#     Threads.@threads for g in 1:ngroups
#         lo = starts[g]
#         g == ngroups ? hi = nrow(ds) : hi = starts[g + 1] - 1
#         special_res[g] = ms[_first_vector_res].second.first(view(_columns(ds)[ms[_first_vector_res].first], lo:hi))
#         new_lengths[g] = length(special_res[g])
#     end
#     total_lengths = sum(new_lengths)
    

    
#     res = AbstractArray[]
#     for j in 1:length(groupcols)
#         _tmpres = similar(ds[!, groupcols[j]], total_lengths)
#         Threads.@threads for i in 1:ngroups
#             for k in 1:new_lengths[i]
#                 _tmpres[(i-1)*new_lengths[i] + k] = _columns(ds)[groupcols[j]][starts[i]]
#             end
#         end
#         push!(res, _tmpres)
#     end
#     nm = _names(ds)[groupcols]
#     for i in 1:length(ms)
#         T = _check_the_output_type(ds, ms[i])
#         _res = Tables.allocatecolumn(T, total_lengths)
#         if i == _first_vector_res
#             Threads.@threads for i1 in 1:ngroups
#                 for k in 1:new_lengths[i1]
#                     _res[(i1-1)*new_lengths[i1] + k] = special_res[i1][k]
#                 end
#             end
#             push!(res, _res)
#             push!(nm, ms[i].second.second)
#             continue
#         end
#         # make sure lo and hi are not defined any where outside the following loops
#         Threads.@threads for g in 1:ngroups
#             lo = starts[g]
#             g == ngroups ? hi = nrow(ds) : hi = starts[g + 1] - 1
#             _res[(g-1)*new_lengths[g]+1:g*new_lengths[g]] .= ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
#             # _res[g] = ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
#          end
#         push!(res, _res)
#         push!(nm, ms[i].second.second)
#     end
#     Dataset(res, nm, copycols = false)
# end
