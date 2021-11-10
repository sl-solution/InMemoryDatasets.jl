function _sortperm_unstable!(idx, x, ranges, last_valid_range, ord, a)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        if (rangeend - rangestart) == 0
            continue
        end
        ds_sort!(x, idx, rangestart, rangeend, a, ord)
    end
end

function _sortperm_unstable_using_hpsort!(idx, x, ranges, last_valid_range, ord, a)
    for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        if (rangeend - rangestart) == 0
            continue
        end
        if rangeend - rangestart + 1 > Threads.nthreads()*2
            hp_ds_sort!(x, idx, a, ord; lo = rangestart, hi = rangeend)
        else
            ds_sort!(x, idx, rangestart, rangeend, a, ord)
        end
    end
end


# NOT OK
# we should find starts here
function fast_sortperm_int_threaded!(x, original_P, copy_P, ranges, rangelen, minval, misatleft, last_valid_range, ::Val{T}) where T
    starts = [T[] for i in 1:Threads.nthreads()]
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        # if (rangeend - rangestart) == 0
        #     continue
        # end
        if misatleft
            _start_vals = _ds_sort_int_missatleft_nopermx!(x, original_P, copy_P, rangestart, rangeend, rangelen, minval, Val(T))
        else
            _start_vals = _ds_sort_int_missatright_nopermx!(x, original_P, copy_P, rangestart, rangeend, rangelen, minval, Val(T))
        end
        _cleanup_starts!(_start_vals, rangeend - rangestart + 1)
        append!(starts[Threads.threadid()], _start_vals .+ rangestart .- 1)
    end
    cnt = 1
    flag = false
    @inbounds for i in 1:length(starts)
        for j in 1:length(starts[i])
            ranges[cnt] = starts[i][j]
            if cnt > 1 && ranges[cnt] < ranges[cnt - 1]
                flag = true
            end
            cnt += 1
        end
    end
    flag && sort!(view(ranges, 1:cnt-1))
    return cnt - 1
end

function fast_sortperm_int_lm!(x, original_P, copy_P, ranges, rangelen, minval, misatleft, last_valid_range, ::Val{T}) where T
    starts = T[]
    for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        # if (rangeend - rangestart) == 0
        #     continue
        # end
        if misatleft
            _start_vals = _ds_sort_int_missatleft_nopermx_threaded_lm!(x, original_P, copy_P, rangestart, rangeend, rangelen, minval, Val(T))
        else
            _start_vals = _ds_sort_int_missatright_nopermx_threaded_lm!(x, original_P, copy_P, rangestart, rangeend, rangelen, minval, Val(T))
        end
        _cleanup_starts!(_start_vals, rangeend - rangestart + 1)
        append!(starts, _start_vals .+ rangestart .- 1)
    end
    @inbounds for i in 1:length(starts)
        ranges[i] = starts[i]
    end
    return length(starts)
end

function fast_sortperm_int!(x, original_P, copy_P, ranges, rangelen, minval, misatleft, last_valid_range, ::Val{T}) where T
    starts = T[]
    for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        # if (rangeend - rangestart) == 0
        #     continue
        # end
        if misatleft
            _start_vals = _ds_sort_int_missatleft_nopermx_threaded!(x, original_P, copy_P, rangestart, rangeend, rangelen, minval, Val(T))
        else
            _start_vals = _ds_sort_int_missatright_nopermx_threaded!(x, original_P, copy_P, rangestart, rangeend, rangelen, minval, Val(T))
        end
        _cleanup_starts!(_start_vals, rangeend - rangestart + 1)
        append!(starts, _start_vals .+ rangestart .- 1)
    end
    @inbounds for i in 1:length(starts)
        ranges[i] = starts[i]
    end
    return length(starts)
end

function _sortperm_int!(idx, idx_cpy, x, ranges, where, last_valid_range, missingatleft, ord, a)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        if (rangeend - rangestart + 1) == 1
            continue
        end
        _minval = stat_minimum(x, lo = rangestart, hi = rangeend)
        if ismissing(_minval)
            continue
        else
            minval::Int = _minval
        end
        maxval::Int = stat_maximum(x, lo = rangestart, hi = rangeend)
        # the overflow is check before calling _sortperm_int!
        rangelen = maxval - minval + 1
        if rangelen < div(rangeend - rangestart + 1, 2)
            if missingatleft
                ds_sort_int_missatleft!(x, idx, idx_cpy, where[Threads.threadid()], rangestart, rangeend, rangelen, minval)
            else
                ds_sort_int_missatright!(x, idx, idx_cpy, where[Threads.threadid()], rangestart, rangeend, rangelen, minval)
            end
        else
            ds_sort!(x, idx, rangestart, rangeend, a, ord)
        end
    end
end

function _apply_by_f_barrier(x::AbstractVector{T}, by, rev) where T
    needrev = rev
    missat = :right
    CT = Core.Compiler.return_type(by∘_date_value, (nonmissingtype(T), ))
    CT = Union{Missing, CT}
    _temp = Vector{CT}(undef, length(x))
    # FIXME this is trouble if counting sort is not going to be used
    if rev && nonmissingtype(CT) <: Signed
        _by = x-> -_date_value(by(x))
        needrev = false
        missat = :left
    else
        _by = x-> _date_value(by(x))
    end
    _temp, _by, needrev, missat
end

missatleftless(x, y) = isless(x, y)
missatleftless(::Missing, y) = true
missatleftless(x, ::Missing) = false
missatleftless(::Missing, ::Missing) = false

function _apply_by!(_temp, x::AbstractVector{T}, idx, _by, rev, needrev, missat) where T
    Threads.@threads for j in 1:length(x)
        # if by(x) is Date or DateTime only grab its value
        @inbounds _temp[j] = _by(x[idx[j]])
    end
    # it is beneficial to check if the value of floats can be converted to Int
    # TODO rev=true should also be considered
    intable = false
    if !rev && eltype(_temp) <: Union{Missing, Float64}
        intable = true
        Threads.@threads for j in 1:length(x)
            @inbounds _is_intable(_temp[j]) && !isequal(_temp[j], -0.0) ? true : (intable = false && break)
        end
    end
    if !rev
        return (_temp, ord(isless, identity, rev, Forward), :right, needrev, intable)
    else
        if missat == :left
            return (_temp, ord(missatleftless, identity, needrev, Forward), missat, needrev, intable)
        else
            return (_temp, ord(isless, identity, needrev, Forward), missat, needrev, intable)
        end
    end
end

function _apply_by(x::AbstractVector, idx, by, rev)
    _temp, _by, needrev, missat = _apply_by_f_barrier(x, by, rev)
    _apply_by!(_temp, x, idx, _by, rev, needrev, missat)
end

function _modify_poolarray_to_integer!(refs, trans)
    @inbounds Threads.@threads for i in 1:length(refs)
        refs[i] = trans[refs[i]]
    end
end


function _fast_path_modify_to_integer!(xrefs, perm)
    @inbounds Threads.@threads for i in 1:length(xrefs)
        xrefs[i] = perm[xrefs[i]]
    end
end


function _fill_idx_for_sort!(idx)
    @inbounds Threads.@threads for i in 1:length(idx)
        idx[i] = i
    end
end

function _check_memory_availability_for_hp_sort(x)
    return 4*sizeof(x) < Base.Sys.free_memory()
end

function _fill_ranges_for_fast_int_sort!(ranges, _starts_vals)
    for kk in 1:length(_starts_vals)
        ranges[kk] = _starts_vals[kk] # TODO we don't need ranges here
    end
end
# T is either Int32 or Int64 based on how many rows ds has
function ds_sort_perm(ds::Dataset, colsidx, by::Vector{<:Function}, rev::Vector{Bool}, a::Base.Sort.Algorithm,  ::Val{T}; skipcol = 0, skipcol_mkcopy = true, notsortpaforjoin = false, givenrange = nothing) where T
    @assert length(colsidx) == length(by) == length(rev) "each col should have all information about lt, by, and rev"
    stable = false
    # arrary to keep the permutation of rows
    if givenrange === nothing
        if skipcol>0 && !skipcol_mkcopy
            # make use of ds.perm space
            idx = index(ds).perm
        else
            idx = Vector{T}(undef, nrow(ds))
        end
        _fill_idx_for_sort!(idx)
    else
        givenrange.idx isa UnitRange ? idx = collect(givenrange.idx) : idx = givenrange.idx
    end

    if givenrange === nothing
        if skipcol>0
            last_valid_range = index(ds).ngroups[]
        else
            last_valid_range::T = 1
        end
    else
        last_valid_range = givenrange.lastvalid
    end
    # ranges keep the starts of blocks of sorted rows
    # rangescpy is a copy which will be help to rearrange ranges in place

    if givenrange === nothing
        if skipcol>0
            if skipcol_mkcopy
                ranges = resize!(copy(index(ds).starts), nrow(ds))
            else # make use of ds.starts
                ranges = resize!(index(ds).starts, nrow(ds))
            end
        else
            ranges = Vector{T}(undef, nrow(ds))
            ranges[1] = 1
        end
    else
        ranges = resize!(givenrange.starts, nrow(ds))
    end

    inbits = zeros(Bool, nrow(ds))


    # rangescpy[1] = 1
    # in case we have integer columns with few distinct values
    int_permcpy = T[]
    int_where = [T[] for _ in 1:Threads.nthreads()]
    int_count = [T[] for _ in 1:Threads.nthreads()]
    for i in 1:length(colsidx)
        if i <= skipcol
            continue
        end
        x = _columns(ds)[colsidx[i]]
        # pooledarray are treating differently (or general case of poolable data)
        if DataAPI.refpool(x) !== nothing
            if by[i] == identity
                _tmp = copy(x)
            else
                _tmp = map(by[i], x)
            end
            if givenrange === nothing
                if i > 1
                    _tmp.refs .= _threaded_permute(_tmp.refs, idx)
                end
            else
                _tmp.refs .= _threaded_permute(_tmp.refs, idx)
            end
            # collect here is to handle Categorical array.
            if !notsortpaforjoin
                if _tmp isa PooledArray
                    _fast_path_modify_to_integer!(_tmp.refs, invperm(sortperm(Dataset(x = DataAPI.refpool(_tmp), copycols = false), :, rev = rev[i], stable = false, alg = a)))
                else
                    # This path is for Categorical array and must be optimised
                    levs = allowmissing(DataAPI.levels(_tmp))
                    push!(levs, missing)
                    rev[i] ? reverse!(levs) : nothing
                    aaa = map(x->get(DataAPI.invrefpool(_tmp), x, missing), levs)
                    trans = Dict{eltype(aaa), Int32}(aaa .=> 1:length(aaa))
                    _modify_poolarray_to_integer!(_tmp.refs, trans)
                end
                _tmp = _tmp.refs
            else # this path is used for sorting (not correct sorting) refs which will be used in joining functions
                _tmp = _tmp.refs
            end
            _ordr = ord(isless, identity, false, Forward)
            _needrev = false
            intable = false
            _missat = :right
        else
            _tmp, _ordr, _missat, _needrev, intable=  _apply_by(x, idx, by[i], rev[i])

        end
        if !_needrev && (eltype(_tmp) <: Union{Missing,Integer} || intable) && eltype(_tmp) !== Missing
            # further check for fast integer sort
            n = length(_tmp)
            if n > 1
                _minval = hp_minimum(_tmp)
                if ismissing(_minval)
                    continue
                else
                    minval::Integer = _minval
                end
                maxval::Integer = hp_maximum(_tmp)
                (diff, o1) = sub_with_overflow(Int(maxval), Int(minval))
                (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
                if !o1 && !o2 && maxval < typemax(Int) && (rangelen <= div(n,2))
                    if last_valid_range == 1
                      #for many levels but not too many we use parallel version
                      if Threads.nthreads() > 1 && nrow(ds) > Threads.nthreads() && rangelen > 100_000 && rangelen*Threads.nthreads() < n

                          if _missat == :left
                              _starts_vals = _ds_sort_int_missatleft_nopermx_threaded!(_tmp, idx, rangelen, minval, Val(T))
                          else
                              _starts_vals = _ds_sort_int_missatright_nopermx_threaded!(_tmp, idx, rangelen, minval, Val(T))
                          end
                          _cleanup_starts!(_starts_vals, n)
                          last_valid_range = length(_starts_vals)
                          _fill_ranges_for_fast_int_sort!(ranges, _starts_vals)
                          if length(colsidx) == 1
                              stable = true
                          end
                          continue
                      elseif Threads.nthreads() > 1 && nrow(ds) > Threads.nthreads() && rangelen > 100_000
                          if _missat == :left
                              _starts_vals = _ds_sort_int_missatleft_nopermx_threaded_lm!(_tmp, idx, rangelen, minval, Val(T))
                          else
                              _starts_vals = _ds_sort_int_missatright_nopermx_threaded_lm!(_tmp, idx, rangelen, minval, Val(T))
                          end
                          _cleanup_starts!(_starts_vals, n)
                          last_valid_range = length(_starts_vals)
                          _fill_ranges_for_fast_int_sort!(ranges, _starts_vals)
                          if length(colsidx) == 1
                              stable = true
                          end
                          continue
                      else

                          if _missat == :left
                              _starts_vals = _ds_sort_int_missatleft_nopermx!(_tmp, idx, rangelen, minval, Val(T))
                          else
                              _starts_vals = _ds_sort_int_missatright_nopermx!(_tmp, idx, rangelen, minval, Val(T))
                          end
                          _cleanup_starts!(_starts_vals, n)
                          last_valid_range = length(_starts_vals)
                          _fill_ranges_for_fast_int_sort!(ranges, _starts_vals)
                          if length(colsidx) == 1
                              stable = true
                          end
                          continue
                      end
                  else
                      resize!(int_permcpy, length(idx))
                      copy!(int_permcpy, idx)
                      if last_valid_range < Threads.nthreads() && rangelen > 100_000

                          last_valid_range = fast_sortperm_int_lm!(_tmp, idx, int_permcpy, ranges, rangelen, minval, _missat == :left, last_valid_range, Val(T))
                          continue
                      end
                      # we should check if there are many observations in each group
                      if n/last_valid_range > 2.0*rangelen && rangelen*Threads.nthreads() < n/2 # if rangelen is not that much

                          last_valid_range = fast_sortperm_int_threaded!(_tmp, idx, int_permcpy, ranges, rangelen, minval, _missat == :left, last_valid_range, Val(T))
                          continue
                      else
                          for nt in 1:Threads.nthreads()
                              resize!(int_where[nt], rangelen + 2)
                          end

                          _sortperm_int!(idx, int_permcpy, _tmp, ranges, int_where, last_valid_range, _missat == :left, _ordr, a)
                      end
                  end
                else
                    if last_valid_range == 1 && Threads.nthreads() > 1 && nrow(ds) > Threads.nthreads()

                        hp_ds_sort!(_tmp, idx, a, _ordr)
                    else
                        if last_valid_range < floor(Int, log2(Threads.nthreads()))/2
                            _sortperm_unstable_using_hpsort!(idx, _tmp, ranges, last_valid_range, _ordr, a)
                        else
                            _sortperm_unstable!(idx, _tmp, ranges, last_valid_range, _ordr, a)
                        end
                    end
                end
            end
        else
            if last_valid_range == 1 && Threads.nthreads() > 1 && nrow(ds) > Threads.nthreads()
                hp_ds_sort!(_tmp, idx, a, _ordr)
            else
                # log2(thcnt) is used because of the current parallel sorting algorithm
                if last_valid_range < floor(Int, log2(Threads.nthreads()))/2
                    _sortperm_unstable_using_hpsort!(idx, _tmp, ranges, last_valid_range, _ordr, a)
                else
                    _sortperm_unstable!(idx, _tmp, ranges, last_valid_range, _ordr, a)
                end
            end
        end
        # last_valid_range = _fill_starts!(ranges, _tmp, rangescpy, last_valid_range, _ordr, Val(T))
        last_valid_range = _fill_starts_v2!(ranges, inbits, _tmp, last_valid_range, _ordr, Val(T))
        last_valid_range == nrow(ds) && return (ranges, idx, last_valid_range, stable)
    end
    return (ranges, idx, last_valid_range, stable)
end

function _stablise_sort!(ranges, idx, last_valid_range, a)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(idx) : rangeend = ranges[i+1] - 1
        if (rangeend - rangestart) == 0
            continue
        end
        # if QuickSort is selected we make sure the worst case scenario is not that much bad
        if rangeend - rangestart + 1 > 1000
            sort!(idx, rangestart, rangeend, MergeSort, Forward)
        else
            sort!(idx, rangestart, rangeend, a, Forward)
        end
    end
end

function _sortperm(ds, cols::MultiColumnIndex, rev::Vector{Bool}; a = HeapSortAlg(), mapformats = true, stable = true, skipcol = 0, skipcol_mkcopy = true, notsortpaforjoin = false, givenrange = nothing)
    colsidx = index(parent(ds))[cols]
    @assert length(colsidx) == length(rev) "`rev` argument must be the same as length of selected columns"
    if _check_for_fast_sort(ds, colsidx, rev, mapformats; notsortpaforjoin = notsortpaforjoin, givenrange = givenrange) == 0
        return copy(_group_starts(ds)), 1:nrow(ds), _ngroups(ds)
    end
    if _check_for_fast_sort(ds, colsidx, rev, mapformats; notsortpaforjoin = notsortpaforjoin, givenrange = givenrange) == 1
        colsidx, ranges, last_valid_index = _find_starts_of_groups(ds, colsidx, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64), mapformats = mapformats)
        if !skipcol_mkcopy
            _fill_idx_for_sort!(_get_sort_perms(ds))
            # make use of perm in original data (simply replace it with new one)
            return ranges, _get_sort_perms(ds), last_valid_index
        else
            return ranges, 1:nrow(ds), last_valid_index
        end
    end
    if skipcol == 0 && _check_for_fast_sort(ds, colsidx, rev, mapformats; notsortpaforjoin = notsortpaforjoin, givenrange = givenrange) == 2
        skipcol = length(_groupcols(ds))
    end
    if skipcol == -1 && _check_for_fast_sort(ds, colsidx, rev, mapformats; notsortpaforjoin = notsortpaforjoin) == 2
        skipcol = length(_groupcols(ds))
    end
    by = Function[]

    # TODO is it safe to refer to parents formats?
    if mapformats
        for j in 1:length(colsidx)
            push!(by, getformat(parent(ds), colsidx[j]))
        end
    else
        for j in 1:length(colsidx)
            push!(by, identity)
        end
    end
    ranges, idx, last_valid_range, isstable = ds_sort_perm(parent(ds), colsidx, by, rev, a, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64); skipcol = skipcol, skipcol_mkcopy = skipcol_mkcopy, notsortpaforjoin = notsortpaforjoin, givenrange = givenrange)
    if stable && !isstable
        if length(idx) == last_valid_range
            return ranges, idx, last_valid_range
        else
            _stablise_sort!(ranges, idx, last_valid_range, a)
            return ranges, idx, last_valid_range
        end
    else
        return ranges, idx, last_valid_range
    end
end

function _sortperm(ds, cols::MultiColumnIndex, rev::Bool = false; a = HeapSortAlg(), mapformats = true, stable = true, skipcol = 0, skipcol_mkcopy = true, notsortpaforjoin = false, givenrange = nothing)
    colsidx = index(parent(ds))[cols]
    revs = repeat([rev], length(colsidx))
    if _check_for_fast_sort(ds, colsidx, revs, mapformats; notsortpaforjoin = notsortpaforjoin, givenrange = givenrange) == 0
        return copy(_group_starts(ds)), 1:nrow(ds), _ngroups(ds)
    end
    if _check_for_fast_sort(ds, colsidx, revs, mapformats; notsortpaforjoin = notsortpaforjoin, givenrange = givenrange) == 1
        colsidx, ranges, last_valid_index = _find_starts_of_groups(ds, colsidx, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64), mapformats = mapformats)

        if !skipcol_mkcopy
            _fill_idx_for_sort!(_get_sort_perms(ds))
            # make use of perm in original data (simply replace it with new one)
            return ranges, _get_sort_perms(ds), last_valid_index
        else
            return ranges, 1:nrow(ds), last_valid_index
        end
    end
    if skipcol == 0 && _check_for_fast_sort(ds, colsidx, revs, mapformats; notsortpaforjoin = notsortpaforjoin, givenrange = givenrange) == 2
        skipcol = length(_groupcols(ds))
    end
    if skipcol == -1 && _check_for_fast_sort(ds, colsidx, revs, mapformats; notsortpaforjoin = notsortpaforjoin) == 2
        skipcol = length(_groupcols(ds))
    end
    _sortperm(parent(ds), cols, revs; a = a , mapformats = mapformats, stable = stable, skipcol = skipcol, skipcol_mkcopy = skipcol_mkcopy, notsortpaforjoin = notsortpaforjoin, givenrange = givenrange)
end

_sortperm(ds, col::ColumnIndex, rev::Bool = false; a = HeapSortAlg(), mapformats = true, stable = true, skipcol = 0, skipcol_mkcopy = true, notsortpaforjoin = false, givenrange = nothing) = _sortperm(ds, [col], rev; a = a,  mapformats = mapformats, stable = stable, skipcol = skipcol, skipcol_mkcopy = skipcol_mkcopy, notsortpaforjoin = notsortpaforjoin, givenrange = givenrange)


function _check_for_fast_sort(ds, colsidx, rev, mapformats; notsortpaforjoin = false, givenrange = nothing)
    scols = _sortedcols(ds)
    revs = _get_rev(ds)
    if revs isa Bool
        revs = repeat([revs], length(scols))
    end
    fmt = _get_fmt(ds)
    notsortpaforjoin && return -1
    givenrange !== nothing && return -1
    if colsidx == scols && rev == revs && mapformats == fmt
        return 0
    elseif length(colsidx) < length(scols)
        if colsidx == view(scols, 1:length(colsidx)) && rev == view(revs, 1:length(colsidx)) && mapformats == fmt
            return 1
        end
    elseif length(colsidx) > length(scols)
        if view(colsidx, 1:length(scols)) == scols && view(rev, 1:length(scols)) == revs && mapformats == fmt
            return 2
        end
    else
        return -1
    end
end
