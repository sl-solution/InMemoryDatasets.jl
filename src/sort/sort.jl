function Base.sortperm(ds::Dataset, cols; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true, threads = true)
    isempty(ds) && return []
    _check_consistency(ds)
    colsidx = index(ds)[cols]
    _sortperm(ds, cols, rev, a = alg, mapformats = mapformats, stable = stable, threads = threads)[2]
end

function Base.sort!(ds::Dataset, cols::MultiColumnIndex; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true, threads = true)
    _check_consistency(ds)
    colsidx = index(ds)[cols]
    if length(rev) == 1
        revs = repeat([rev], length(colsidx))
    else
        revs = rev
    end

    @assert length(colsidx) == length(revs) "the reverse argument must be the same length as the length of selected columns"
    if isempty(ds)
        _reset_grouping_info!(ds)
        append!(index(ds).sortedcols, collect(colsidx))
        append!(index(ds).rev, revs)
        index(ds).fmt[] = mapformats
        return ds
    end

    _check_for_fast_sort(ds, colsidx, revs, mapformats) == 0 && return ds
    _use_ds_perm = false
    if _check_for_fast_sort(ds, colsidx, revs, mapformats) == 1
        _use_ds_perm = true
    end
    skipcol = 0
    if _check_for_fast_sort(ds, colsidx, revs, mapformats) == 2
        skipcol = length(index(ds).sortedcols)
    end
    starts, perm, ngroups = _sortperm(ds, cols, revs; a = alg, mapformats = mapformats, stable = stable, skipcol = skipcol, skipcol_mkcopy = false, threads = threads)
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
    _permute_ds_after_sort!(ds, perm; threads = threads)
    ds
end


Base.sort!(ds::Dataset, col::ColumnIndex; alg = HeapSortAlg(), rev::Bool = false, mapformats::Bool = true, stable =true, threads = true) = sort!(ds, [col], rev = rev, alg = alg, mapformats = mapformats, stable = stable, threads = threads)


function Base.sort(ds::Dataset, cols::MultiColumnIndex; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true, threads = true)
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
    starts, perm, ngroups = _sortperm(ds, cols, revs; a = alg, mapformats = mapformats, stable = stable, skipcol = skipcol, skipcol_mkcopy = true, threads = threads)
    newds = ds[perm, :]
    _reset_grouping_info!(newds)
    append!(index(newds).sortedcols, collect(colsidx))
    append!(index(newds).rev, revs)
    if !(perm isa Vector)
        append!(index(newds).perm, collect(perm))
    else
        append!(index(newds).perm, perm)
    end
    append!(index(newds).starts, starts)
    index(newds).ngroups[] = ngroups
    index(newds).fmt[] = mapformats
    newds
end


Base.sort(ds::Dataset, col::ColumnIndex; alg = HeapSortAlg(), rev::Bool = false, mapformats::Bool = true, stable =true, threads = true) = sort(ds, [col], rev = rev, alg = alg, mapformats = mapformats, stable = stable, threads = threads)

function Base.sort(ds::SubDataset, cols::MultiColumnIndex; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true, view = false, threads = true)
    _check_consistency(ds)
    colsidx = index(ds)[cols]
    if rev isa AbstractVector
        @assert length(rev) == length(colsidx) "length of rev and the number of selected columns must match"
        revs = rev
    else
        revs = repeat([rev], length(colsidx))
    end
    starts, idx, last_valid_range =  _sortperm_v(ds, cols, revs, stable = stable, a = alg, mapformats = mapformats, threads = true)
    if view
        Base.view(ds, idx, :)
    else
        newds = ds[idx, :]
        append!(index(newds).sortedcols, collect(colsidx))
        append!(index(newds).rev, revs)
        append!(index(newds).perm, idx)
        append!(index(newds).starts, starts)
        index(newds).ngroups[] = last_valid_range
        index(newds).fmt[] = mapformats
        newds
    end
end

Base.sort(ds::SubDataset, col::ColumnIndex; alg = HeapSortAlg(), rev::Bool = false, mapformats::Bool = true, stable =true, threads = true, view = false) = sort(ds, [col], rev = rev, alg = alg, mapformats = mapformats, stable = stable, threads = threads, view = view)

function Base.sortperm(ds::SubDataset, cols; alg = HeapSortAlg(), rev = false, mapformats::Bool = true, stable = true, threads = true)
    isempty(ds) && return []
    _check_consistency(ds)
    colsidx = index(ds)[cols]
    _sortperm_v(ds, cols, rev, a = alg, mapformats = mapformats, stable = stable, threads = threads)[2]
end

Base.sort(ds::AbstractDataset; kwargs...) = throw(ArgumentError("pass the sorting columns as the second argument"))
Base.sortperm(ds::AbstractDataset; kwargs...) = throw(ArgumentError("pass the sorting columns as the second argument"))


function unsort!(ds::Dataset; threads = true)
    isempty(ds) && return ds
    if isempty(index(ds).perm) #if perm is empty everything else should be empty, here we just make sure
        empty!(index(ds).sortedcols)
        empty!(index(ds).rev)
        empty!(index(ds).starts)
        index(ds).ngroups[] = 1
        index(ds).grouped[] = false
        return ds
    else
        _permute_ds_after_sort!(ds, invperm(index(ds).perm); threads = threads)
        # TODO we may don't need to reset grouping info
        _reset_grouping_info!(ds)
        ds
    end
end

function Base.issorted(ds::AbstractDataset, cols::MultiColumnIndex; rev = false, mapformats = true, threads = true)
    _issorted(ds, cols, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64), rev = rev, mapformats = mapformats, threads = threads)[1]
end
Base.issorted(ds::AbstractDataset, col::ColumnIndex; rev = false, mapformats = true, threads = true) = issorted(ds, [col], rev = rev, mapformats = mapformats, threads = threads)

function issorted!(ds::Dataset, cols::MultiColumnIndex; rev = false, mapformats = true, threads = true)
    res, starts, lastvalid, colsidx, revs, mapformats = _issorted(ds, cols, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64), rev = rev, mapformats = mapformats, threads = threads)
    if res
        _reset_grouping_info!(ds)
        append!(index(ds).sortedcols, collect(colsidx))
        append!(index(ds).rev, revs)
        append!(index(ds).perm, collect(1:nrow(ds)))
        append!(index(ds).starts, starts)
        index(ds).ngroups[] = lastvalid
        index(ds).fmt[] = mapformats
    end
    res
end

issorted!(ds::Dataset, col::ColumnIndex; rev = false, mapformats = true, threads = true) = issorted!(ds, [col], rev = rev, mapformats = mapformats, threads = threads)

function _issorted(ds, cols::MultiColumnIndex, ::Val{T}; rev = false, mapformats = true, threads = true) where T
    colsidx = index(ds)[cols]
    if rev isa AbstractVector
        @assert length(rev) == length(colsidx) "length of rev and the number of selected columns must match"
        revs = rev
    else
        revs = repeat([rev], length(colsidx))
    end
    by = Function[]

    if mapformats
        for j in 1:length(colsidx)
            push!(by, getformat(parent(ds), colsidx[j]))
        end
    else
        for j in 1:length(colsidx)
            push!(by, identity)
        end
    end
    res = true
    starts = Vector{T}(undef, nrow(ds))
    starts[1] = 1
    lastvalid = 1
    inbits = zeros(Bool, nrow(ds))
    inbits[1] = true
    for j in 1:length(colsidx)
        v = _columns(ds)[colsidx[j]]
        _ord =  Base.Order.ord(isless, by[j], revs[j])
        part_res = _issorted_check_for_each_range(v, starts, lastvalid, _ord, nrow(ds); threads = threads)
        !part_res && return false, starts, lastvalid, colsidx, revs, mapformats
        _find_starts_of_groups!(_columns(ds)[colsidx[j]], 1:nrow(ds), by[j], inbits; threads = threads)
        lastvalid = _fill_starts_from_inbits!(starts, inbits)
        lastvalid == nrow(ds) && return true, starts, lastvalid, colsidx, revs, mapformats
        # lastvalid = _fill_starts_v2!(starts, inbits, _columns(ds)[colsidx[j]], lastvalid, Base.Order.ord(isless, by[j], revs[j]), Val(T))
    end
    res, starts, lastvalid, colsidx, revs, mapformats
end

function _issorted_check_for_each_range(v, starts, lastvalid, _ord, nrows; threads = true)
    part_res = ones(Bool, threads ? Threads.nthreads() : 1)
    @_threadsfor threads for rng in 1:lastvalid
        lo = starts[rng]
        rng == lastvalid ? hi = nrows : hi = starts[rng+1] - 1
        part_res[Threads.threadid()] = _issorted_barrier(v, _ord, lo, hi)
        !part_res[Threads.threadid()] &&  break
    end
    all(part_res)
end

function _fill_starts_from_inbits!(starts, inbits)
    lastvalid = 1
    @inbounds for i in 1:length(inbits)
        if inbits[i]
            starts[lastvalid] = i
            lastvalid += 1
        end
    end
    lastvalid - 1
end

function _issorted_barrier(v, _ord, lo, hi)
    lo >= hi && return true
    @inbounds for i in lo+1:hi
        Base.Order.lt(_ord, v[i], v[i-1]) && return false
    end
    true
end
