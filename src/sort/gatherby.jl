using .Base: sub_with_overflow, add_with_overflow, mul_with_overflow

# FIXME it needs more work, e.g it needs to handle missing values
function _gatherby(ds, cols, ::Val{T}; mapformats = true) where T
    colidx = index(ds)[cols]
    n = nrow(ds)
    flag = false
    _max_level = nrow(ds)
    prev_max_group = UInt(1)
    prev_groups = ones(UInt, nrow(ds))
    groups = Vector{T}(undef, nrow(ds))
    seen_non_int = false

    rhashes = Vector{UInt}(undef, 1)
    sz = max(1 + ((5 * _max_level) >> 2), 16)
    sz = 1 << (8 * sizeof(sz) - leading_zeros(sz - 1))
    @assert 4 * sz >= 5 * _max_level
    gslots = Vector{T}(undef, 1)

    for j in 1:length(colidx)
        _f = identity
        if mapformats
            _f = getformat(ds, colidx[j])
        end
        _f = _date_value∘_f

        CT = Core.Compiler.return_type(_f, (eltype(_columns(ds)[colidx[j]]),))
        # assuming format is not changing the number of levels in pooled array
        if DataAPI.refpool(_columns(ds)[colidx[j]]) !== nothing
            v = DataAPI.refarray(_columns(ds)[colidx[j]])
            minval = hp_minimum(_f, v)
            if ismissing(minval)
                continue
            end
            maxval = hp_maximum(_f, v)
            (diff, o1) = sub_with_overflow(maxval, minval)
            (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
            (outmult, o3) = mul_with_overflow(Int(rangelen), Int(prev_max_group))
            if !o1 && !o2 && !o3 && maxval < typemax(Int) && (rangelen < div(n,2)) && prev_max_group*rangelen < 2*length(v)
                flag_out, prev_max_group = _grouper_for_int_pool!(prev_groups, groups, prev_max_group, v, _f, minval, rangelen)
            else
                if !seen_non_int
                    resize!(rhashes, nrow(ds))
                    resize!(gslots, sz)
                    seen_non_int = true
                end
                flag_out, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
            end
            flag = flag_out

        elseif CT <: Integer
            v = _columns(ds)[colidx[j]]
            minval = hp_minimum(_f, v)
            if ismissing(minval)
                continue
            end
            maxval = hp_maximum(_f, v)
            (diff, o1) = sub_with_overflow(maxval, minval)
            (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
            (outmult, o3) = mul_with_overflow(Int(rangelen), Int(prev_max_group))
            if !o1 && !o2 && !o3 && maxval < typemax(Int) && (rangelen < div(n,2)) && prev_max_group*rangelen < 2*length(v)
                flag_out, prev_max_group = _grouper_for_int_pool!(prev_groups, groups, prev_max_group, v, _f, minval, rangelen)
            else
                if !seen_non_int
                    resize!(rhashes, nrow(ds))
                    resize!(gslots, sz)
                    seen_non_int = true
                end
                flag_out, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
            end
            flag = flag_out
        else
            if !seen_non_int
                resize!(rhashes, nrow(ds))
                resize!(gslots, sz)
                seen_non_int = true
            end
            v = _columns(ds)[colidx[j]]

            flag_out, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
            flag = flag_out
        end
        !flag && break
    end
    return groups, Int(prev_max_group)
end

struct GatherBy
    parent::Dataset
    groupcols::Vector{Int}
    groups::Vector{<:Integer}
    ngroups::Int
    mapformats::Bool
end

function gatherby(ds::Dataset, cols::MultiColumnIndex; mapformats = true)
    colsidx = index(ds)[cols]
    a = _gatherby(ds, colsidx, nrow(ds)<typemax(Int32) ? Val(Int32) : Val(Int64), mapformats = mapformats)
    GatherBy(ds, colsidx, a[1], a[2], mapformats)
end
gatherby(ds::Dataset, col::ColumnIndex; mapformats = true) = gatherby(ds, [col], mapformats = mapformats)

function compute_indices(groups::AbstractVector{<:Integer}, ngroups::Integer)
    # count elements in each group
    stops = zeros(Int, ngroups+1)
    @inbounds for gix in groups
        stops[gix+1] += 1
    end

    # group start positions in a sorted table
    starts = Vector{Int}(undef, ngroups+1)
    if length(starts) > 0
        starts[1] = 1
        @inbounds for i in 1:ngroups
            starts[i+1] = starts[i] + stops[i]
        end
    end

    # define row permutation that sorts them into groups
    rperm = Vector{Int}(undef, length(groups))
    copyto!(stops, starts)
    @inbounds for (i, gix) in enumerate(groups)
        rperm[stops[gix+1]] = i
        stops[gix+1] += 1
    end
    stops .-= 1

    # When skipmissing=true was used, group 0 corresponds to missings to drop
    # Otherwise it's empty
    popfirst!(starts)
    popfirst!(stops)

    return rperm, starts, stops
end

Base.summary(gds::GatherBy) =
        @sprintf("%d×%d Gathered Dataset, Gathered by: %s", size(gds.parent)..., join(_names(gds.parent)[gds.groupcols], " ,"))


function Base.show(io::IO, gds::GatherBy;

                   kwargs...)
     _show(io, view(gds.parent,compute_indices(gds.groups, gds.ngroups)[1], :); title = summary(gds), kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", gds::GatherBy;
          kwargs...) =
    show(io, gds; title = summary(gds), kwargs...)


function _fill_gathered_col!(x, y, loc)
    Threads.@threads for i in 1:length(y)
        x[loc[i]] = y[i]
    end
end

function _fill_mapreduce_col!(x, f, op, init0, y, loc)
    init = init0[1, 1]
    Threads.@threads for i in 1:length(y)
        init0[loc[i], Threads.threadid()] =  op(init0[loc[i],Threads.threadid()], f(y[i]))
    end
    Threads.@threads for i in 1:length(x)
        x[i] = mapreduce(identity, op, init0[i, :], init = init)
    end
end

function Base.mapreduce(gds::GatherBy, f, op, col::ColumnIndex, init::T) where T
    init0 = fill(init, gds.ngroups, Threads.nthreads())
    res = AbstractVector[]
    for j in gds.groupcols
        push!(res, Tables.allocatecolumn(eltype(_columns(gds.parent)[j]), gds.ngroups))
    end
    push!(res, Tables.allocatecolumn(Union{T, Missing}, gds.ngroups))
    for j in 1:(length(res)-1)
        _fill_gathered_col!(res[j], _columns(gds.parent)[gds.groupcols[j]], gds.groups)
    end
    _fill_mapreduce_col!(res[end], f, op, init0, _columns(gds.parent)[index(gds.parent)[col]], gds.groups)
    newnm = _names(gds.parent)[gds.groupcols]
    push!(newnm, Symbol(_names(gds.parent)[index(gds.parent)[col]], "_agg"))
    newds = Dataset(res, newnm, copycols = false)
    for j in 1:(length(res)-1)
        setformat!(newds, j, getformat(gds.parent, gds.groupcols[j]))
    end
    newds
end


function combine(gds::GatherBy, @nospecialize(args...))
    idx_cpy::Index = Index(copy(index(gds.parent).lookup), copy(index(gds.parent).names), Dict{Int, Function}())
    ms = normalize_combine_multiple!(idx_cpy, index(gds.parent), args...)
    # the rule is that in combine, byrow must only be used for already aggregated columns
    # so, we should check every thing pass to byrow has been assigned in args before it
    # if this is not the case, throw ArgumentError and ask user to use modify instead
    newlookup, new_nm = _create_index_for_newds(gds.parent, ms, gds.groupcols)
    !(_is_byrow_valid(Index(newlookup, new_nm, Dict{Int, Function}()), ms)) && throw(ArgumentError("`byrow` must be used for aggregated columns, use `modify` otherwise"))
    # _check_mutliple_rows_for_each_group return the first transformation which causes multiple
    # rows or 0 if all transformations return scalar for each group
    # the transformation returning multiple rows must not be based on the previous columns in combine
    # result (which seems reasonable ??)
    _first_vector_res = _check_mutliple_rows_for_each_group(gds.parent, ms)

    _is_groupingcols_modifed(gds.parent, ms) && throw(ArgumentError("`combine` cannot modify the grouping columns"))

    groupcols::Vector{Int} = gds.groupcols
    a = compute_indices(gds.groups, gds.ngroups)
    starts::Vector{Int} = a[2]
    ngroups::Int = gds.ngroups

    # we will use new_lengths later for assigning the grouping info of the new ds
    if _first_vector_res == 0
        new_lengths = ones(Int, ngroups)
        cumsum!(new_lengths, new_lengths)
        total_lengths = ngroups
    else
        CT = return_type(ms[_first_vector_res].second.first,
                 gds.parent[!, ms[_first_vector_res].first].val)
        special_res = Vector{CT}(undef, ngroups)
        new_lengths = Vector{Int}(undef, ngroups)
        # _columns(ds)[ms[_first_vector_res].first]
        _compute_the_mutli_row_trans!(special_res, new_lengths, view(_columns(gds.parent)[index(gds.parent)[ms[_first_vector_res].first]], a[1]), nrow(gds.parent), ms[_first_vector_res].second.first, _first_vector_res, starts, ngroups)
        # special_res, new_lengths = _compute_the_mutli_row_trans(ds, ms, _first_vector_res, starts, ngroups)
        cumsum!(new_lengths, new_lengths)
        total_lengths = new_lengths[end]
    end
    all_names = _names(gds.parent)

    newds_idx = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}(), Int[], Bool[], false, [], Int[], 1)

    newds = Dataset([], newds_idx)
    newds_lookup = index(newds).lookup
    var_cnt = 1
    for j in 1:length(groupcols)
        _push_groups_to_res!(_columns(newds), Tables.allocatecolumn(eltype(gds.parent[!, groupcols[j]].val), total_lengths), view(_columns(gds.parent)[groupcols[j]], a[1]), starts, new_lengths, total_lengths, j, groupcols, ngroups)
        push!(index(newds), new_nm[var_cnt])
        setformat!(newds, new_nm[var_cnt] => get(index(gds.parent).format, groupcols[j], identity))
        var_cnt += 1
    end
    for i in 1:length(ms)
        if i == _first_vector_res
            _combine_f_barrier_special(special_res, view(gds.parent[!, ms[i].first].val, a[1]), newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, _first_vector_res,ngroups, new_lengths, total_lengths)
        else
            _combine_f_barrier((!haskey(newds_lookup, ms[i].first) && !(ms[i].second.first isa Expr)) ? view(_columns(gds.parent)[index(gds.parent)[ms[i].first]], a[1]) : view(_columns(gds.parent)[1], a[1]), newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, starts, ngroups, new_lengths, total_lengths)
        end
        if !haskey(index(newds), ms[i].second.second)
            push!(index(newds), ms[i].second.second)
        end

    end
    # grouping information for the output dataset
    # append!(index(newds).sortedcols, index(newds)[index(ds).names[index(gds.parent).sortedcols]])
    # append!(index(newds).rev, index(ds).rev)
    # append!(index(newds).perm, collect(1:total_lengths))
    # index(newds).grouped[] = true
    # index(newds).ngroups[] = ngroups
    # append!(index(newds).starts, collect(1:total_lengths))
    # for i in 2:(length(new_lengths))
    #     index(newds).starts[i] = new_lengths[i - 1]+1
    # end
    newds
end
