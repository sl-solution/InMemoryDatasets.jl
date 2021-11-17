function Base.sortperm(ds::Dataset, cols; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true)
    isempty(ds) && return []
    _check_consistency(ds)
    colsidx = index(ds)[cols]
    _sortperm(ds, cols, rev, a = alg, mapformats = mapformats, stable = stable)[2]
end

function Base.sort!(ds::Dataset, cols::MultiColumnIndex; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true)
    isempty(ds) && return ds
    _check_consistency()
    colsidx = index(ds)[cols]
    if length(rev) == 1
        revs = repeat([rev], length(colsidx))
    else
        revs = rev
    end

    @assert length(colsidx) == length(revs) "the reverse argument must be the same length as the length of selected columns"
    _check_for_fast_sort(ds, colsidx, revs, mapformats) == 0 && return ds
    _use_ds_perm = false
    if _check_for_fast_sort(ds, colsidx, revs, mapformats) == 1
        _use_ds_perm = true
    end
    skipcol = 0
    if _check_for_fast_sort(ds, colsidx, revs, mapformats) == 2
        skipcol = length(index(ds).sortedcols)
    end
    starts, perm, ngroups = _sortperm(ds, cols, revs; a = alg, mapformats = mapformats, stable = stable, skipcol = skipcol, skipcol_mkcopy = false)
    if _use_ds_perm || skipcol>0 #index(ds).perm and index(ds).starts already been updated
        empty!(index(ds).sortedcols)
        empty!(index(ds).rev)
        append!(index(ds).sortedcols, collect(colsidx))
        append!(index(ds).rev, revs)
        index(ds).ngroups[] = ngroups
        index(ds).fmt[] = mapformats
        if _use_ds_perm # current implementation needs to reset starts and use the computed one/ it should be optimised
            empty!(index(ds).starts)
            append!(index(ds).starts, starts)
        end
    elseif skipcol==0
        _reset_grouping_info!(ds)
        append!(index(ds).sortedcols, collect(colsidx))
        append!(index(ds).rev, revs)
        append!(index(ds).perm, perm)
        append!(index(ds).starts, starts)
        index(ds).ngroups[] = ngroups
        index(ds).fmt[] = mapformats
    end

    _modified(_attributes(ds))
    _permute_ds_after_sort!(ds, perm)
    ds
end


Base.sort!(ds::Dataset, col::ColumnIndex; alg = HeapSortAlg(), rev::Bool = false, mapformats::Bool = true, stable =true) = sort!(ds, [col], rev = rev, alg = alg, mapformats = mapformats, stable = stable)


function Base.sort(ds::Dataset, cols::MultiColumnIndex; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true)
    isempty(ds) && return copy(ds)
    _check_consistency(ds)
    colsidx = index(ds)[cols]
    if length(rev) == 1
        revs = repeat([rev], length(colsidx))
    else
        revs = rev
    end

    @assert length(colsidx) == length(revs) "the reverse argument must be the same length as the length of selected columns"
    _check_for_fast_sort(ds, colsidx, revs, mapformats) == 0 && return copy(ds)
    skipcol = 0
    if _check_for_fast_sort(ds, colsidx, revs, mapformats) == 2
        skipcol = length(index(ds).sortedcols)
    end
    starts, perm, ngroups = _sortperm(ds, cols, revs; a = alg, mapformats = mapformats, stable = stable, skipcol = skipcol, skipcol_mkcopy = true)
    newds = ds[perm, :]
    _reset_grouping_info!(newds)
    append!(index(newds).sortedcols, collect(colsidx))
    append!(index(newds).rev, revs)
    append!(index(newds).perm, collect(perm))
    append!(index(newds).starts, starts)
    index(newds).ngroups[] = ngroups
    index(newds).fmt[] = mapformats
    newds
end


Base.sort(ds::Dataset, col::ColumnIndex; alg = HeapSortAlg(), rev::Bool = false, mapformats::Bool = true, stable =true) = sort(ds, [col], rev = rev, alg = alg, mapformats = mapformats, stable = stable)


function unsort!(ds::Dataset)
    isempty(ds) && return ds
    if isempty(index(ds).perm) #if perm is empty everything else should be empty, here we just make sure
        empty!(index(ds).sortedcols)
        empty!(index(ds).rev)
        empty!(index(ds).starts)
        index(ds).ngroups[] = 1
        index(ds).grouped[] = false
        return ds
    else
        _permute_ds_after_sort!(ds, invperm(index(ds).perm))
        # TODO we may don't need to reset grouping info
        _reset_grouping_info!(ds)
        ds
    end
end
