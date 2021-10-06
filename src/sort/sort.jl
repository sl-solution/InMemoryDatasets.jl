function Base.sortperm(ds::Dataset, cols; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true)
    isempty(ds) && return []
    colsidx = index(ds)[cols]
    _sortperm(ds, cols, rev, a = alg, mapformats = mapformats, stable = stable)[2]
end

function Base.sort!(ds::Dataset, cols::MultiColumnIndex; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true)
    isempty(ds) && return ds
    colsidx = index(ds)[cols]
    if length(rev) == 1
        revs = repeat([rev], length(colsidx))
    else
        revs = rev
    end

    @assert length(colsidx) == length(revs) "the reverse argument must be the same length as the length of selected columns"
    _check_for_fast_sort(ds, colsidx, revs, mapformats) == 0 && return ds

    # if issorted
    #     index(ds).sortedcols == colsidx && index(ds).rev == revs && return ds
    #     selected_columns, ranges, last_valid_index = _find_starts_of_groups(ds::Dataset, colsidx, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
    #     _reset_grouping_info!(ds)
    #     append!(index(ds).sortedcols, selected_columns)
    #     append!(index(ds).rev, revs)
    #     append!(index(ds).perm, collect(1:nrow(ds)))
    #     append!(index(ds).starts, ranges)
    #     index(ds).ngroups[] = last_valid_index
    #     _modified(_attributes(ds))
    #     ds
    # else
        starts, perm, ngroups = _sortperm(ds, cols, revs; a = alg, mapformats = mapformats, stable = stable)
        _reset_grouping_info!(ds)
        append!(index(ds).sortedcols, collect(colsidx))
        append!(index(ds).rev, revs)
        append!(index(ds).perm, perm)
        append!(index(ds).starts, starts)
        index(ds).ngroups[] = ngroups
        index(ds).fmt[] = mapformats
        _modified(_attributes(ds))
        _permute_ds_after_sort!(ds, perm)
    # end
    ds
end


Base.sort!(ds::Dataset, col::ColumnIndex; alg = HeapSortAlg(), rev::Bool = false, mapformats::Bool = true, stable =true) = sort!(ds, [col], rev = rev, alg = alg, mapformats = mapformats, stable = stable)


function Base.sort(ds::Dataset, cols::MultiColumnIndex; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true)
    isempty(ds) && return copy(ds)
    colsidx = index(ds)[cols]
    if length(rev) == 1
        revs = repeat([rev], length(colsidx))
    else
        revs = rev
    end

    @assert length(colsidx) == length(revs) "the reverse argument must be the same length as the length of selected columns"
    _check_for_fast_sort(ds, colsidx, revs, mapformats) == 0 && return copy(ds)

    # if issorted
    #     index(ds).sortedcols == colsidx && index(ds).rev == revs && return copy(ds)
    #     selected_columns, ranges, last_valid_index = _find_starts_of_groups(ds::Dataset, colsidx, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
    #     newds = copy(ds)
    #     _reset_grouping_info!(newds)
    #     append!(index(newds).sortedcols, selected_columns)
    #     append!(index(newds).rev, revs)
    #     append!(index(newds).perm, collect(1:nrow(newds)))
    #     append!(index(newds).starts, ranges)
    #     index(newds).ngroups[] = last_valid_index
    # else
        starts, perm, ngroups = _sortperm(ds, cols, revs; a = alg, mapformats = mapformats, stable = stable)
        newds = ds[perm, :]
        _reset_grouping_info!(newds)
        append!(index(newds).sortedcols, collect(colsidx))
        append!(index(newds).rev, revs)
        append!(index(newds).perm, perm)
        append!(index(newds).starts, starts)
        index(newds).ngroups[] = ngroups
        index(newds).fmt[] = mapformats
    # end
    newds
end


Base.sort(ds::Dataset, col::ColumnIndex; alg = HeapSortAlg(), rev::Bool = false, mapformats::Bool = true, stable =true) = sort(ds, [col], rev = rev, alg = alg, mapformats = mapformats, stable = stable)


function unsort!(ds::Dataset)
    isempty(ds) && return ds
    if isempty(index(ds).perm)
        return ds
    else
        _permute_ds_after_sort!(ds, invperm(index(ds).perm))
        # TODO we may don't need to reset grouping info
        _reset_grouping_info!(ds)
        ds
    end
end
