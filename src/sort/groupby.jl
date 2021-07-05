function groupby!(ds::Dataset, cols::MultiColumnIndex; rev = false, issorted = false)
    if issorted
        sort!(ds, cols, rev = rev, issorted = issorted)
        index(ds).grouped[] = true
        _modified(_attributes(ds))
        ds
    else
        sort!(ds, cols, rev = rev, issorted = issorted)
        index(ds).grouped[] = true
        _modified(_attributes(ds))
        ds
    end
end

groupby!(ds::Dataset, col::ColumnIndex; rev = false, issorted = false) = groupby!(ds, [col]; rev = rev, issorted = issorted)

struct GroupBy
    parent::Dataset
    groupcols
    perm
    starts
    lastvalid
end

function groupby(ds::Dataset, cols::MultiColumnIndex; rev = false)
    colsidx = index(ds)[cols]
    a = _sortperm(ds, cols, rev)
    GroupBy(ds,colsidx, a[2], a[1], a[3])
end

groupby(ds::Dataset, col::ColumnIndex; rev = false) = groupby(ds, [col], rev = rev)


function combine(gds::GroupBy, @nospecialize(args...))
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
    a = (gds.perm, gds.starts, gds.lastvalid)
    starts::Vector{Int} = a[2]
    ngroups::Int = gds.lastvalid

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


Base.summary(gds::GroupBy) =
        @sprintf("%d×%d View of Grouped Dataset, Grouped by: %s", size(gds.parent)..., join(_names(gds.parent)[gds.groupcols], " ,"))


function Base.show(io::IO, gds::GroupBy;

                   kwargs...)
     _show(io, view(gds.parent, gds.perm, :); title = summary(gds), kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", gds::GroupBy;
          kwargs...) =
    show(io, gds; title = summary(gds), kwargs...)


function ungroup!(ds::Dataset)
    if index(ds).grouped[]
        index(ds).grouped[] = false
        _modified(_attributes(ds))
    end
    ds
end

isgrouped(ds::Dataset)::Bool = index(ds).grouped[]
isgrouped(ds::SubDataset)::Bool = false

function group_starts(ds::Dataset)
    index(ds).starts[1:index(ds).ngroups[]]
end
function getindex_group(ds::Dataset, i::Integer)
    if !(1 <= i <= index(ds).ngroups[])
        throw(BoundsError(ds, i))
    end
    lo = index(ds).starts[i]
    i == index(ds).ngroups[] ? hi = nrow(ds) : hi = index(ds).starts[i+1] - 1
    lo:hi
end
