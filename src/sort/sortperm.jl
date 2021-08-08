function _sortperm_unstable!(idx, x, ranges, last_valid_range, ord)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        if (rangeend - rangestart) == 0
            continue
        end
        ds_sort!(x, idx, rangestart, rangeend, QuickSort, ord)
    end
end


function _sortperm_pooledarray!(idx, idx_cpy, x, xpool, where, counts, ranges, last_valid_range, rev)
    ngroups = length(xpool)
    perm = sortperm(xpool, rev = rev)
    iperm = invperm(perm)
    Threads.@threads for i in 1:last_valid_range
        lo = ranges[i]
        i == last_valid_range ? hi = length(x) : hi = ranges[i+1] - 1
        if (hi - lo) == 0
            continue
        end
        _group_indexer!(x::Vector, idx, idx_cpy, where[Threads.threadid()], counts[Threads.threadid()], lo, hi, ngroups, perm, iperm)
    end
end


function _sortperm_int!(idx, idx_cpy, x, ranges, where, last_valid_range, missingatleft, ord)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(x) : rangeend = ranges[i+1] - 1
        if (rangeend - rangestart + 1) == 1
            continue
        end
        minval::Int = stat_minimum(x, lo = rangestart, hi = rangeend)
        if ismissing(minval)
            continue
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
            ds_sort!(x, idx, rangestart, rangeend, QuickSort, ord)
        end
    end
end

# Date & Time should be treated as integer
_date_value(x::TimeType) = Dates.value(x)
_date_value(x::Period) = Dates.value(x)
_date_value(x) = x


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

function _apply_by!(_temp, x::Vector{T}, idx, _by, rev, needrev, missat) where T
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
    Threads.@threads for i in 1:length(refs)
        refs[i] = trans[refs[i]]
    end
end


function _fast_path_modify_to_integer!(xrefs, perm)
    Threads.@threads for i in 1:length(xrefs)
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
# T is either Int32 or Int64 based on how many rows ds has
function ds_sort_perm(ds::Dataset, colsidx, by::Vector{<:Function}, rev::Vector{Bool}, ::Val{T}) where T
    @assert length(colsidx) == length(by) == length(rev) "each col should have all information about lt, by, and rev"
    # prv_gc = GC.enable(false)
    # ordr = ord(lt,identity,rev,order)

    # arrary to keep the permutation of rows
    idx = Vector{T}(undef, nrow(ds))
    _fill_idx_for_sort!(idx)

    # ranges keep the starts of blocks of sorted rows
    # rangescpy is a copy which will be help to rearrange ranges in place
    ranges = Vector{T}(undef, nrow(ds))
    # FIXME there is no need for this rangescpy if there is only one column
    # rangescpy = Vector{T}(undef, nrow(ds))
    inbits = zeros(Bool, nrow(ds))

    last_valid_range::T = 1

    ranges[1] = 1
    # rangescpy[1] = 1
    # in case we have integer columns with few distinct values
    int_permcpy = T[]
    int_where = [T[] for _ in 1:Threads.nthreads()]
    int_count = [T[] for _ in 1:Threads.nthreads()]

    for i in 1:length(colsidx)
        x = _columns(ds)[colsidx[i]]
        # pooledarray are treating differently (or general case of poolable data)
        if DataAPI.refpool(x) !== nothing
            if by[i] == identity
                _tmp = copy(x)
            else
                _tmp = map(by[i], x)
            end
            if i > 1
                _tmp.refs .= _threaded_permute(_tmp.refs, idx)
            end
            # collect here is to handle Categorical array.
            if _tmp isa PooledArray
                _fast_path_modify_to_integer!(_tmp.refs, invperm(sortperm(Dataset(x = DataAPI.refpool(_tmp), copycols = false), :, rev = rev[i])))
            else
                # This path is for Categorical array and must be optimised
                aaa = map(x->get(DataAPI.invrefpool(_tmp), x, missing), sort!(Dataset(x = collect(DataAPI.refpool(_tmp))), :, rev = rev[i]).x.val)
                trans = Dict{eltype(aaa), Int32}(aaa .=> 1:length(aaa))
                _modify_poolarray_to_integer!(_tmp.refs, trans)
            end
            _ordr = ord(isless, identity, false, Forward)
            _tmp = _tmp.refs
            _needrev = false
            intable = false
            _missat = :right
        else
            _tmp, _ordr, _missat, _needrev, intable=  _apply_by(x, idx, by[i], rev[i])
        #     rangelen = length(DataAPI.refpool(_tmp))
        #     for nt in 1:Threads.nthreads()
        #         resize!(int_where[nt], rangelen + 2)
        #         resize!(int_count[nt], rangelen + 2)
        #     end
        #     resize!(int_permcpy, length(idx))
        #     copy!(int_permcpy, idx)
        #     _ordr = ord(isless, identity, rev[i], Forward)
        #     _sortperm_pooledarray!(idx, int_permcpy, DataAPI.refarray(_tmp), DataAPI.refpool(_tmp), int_where, int_count, ranges, last_valid_range, rev[i])
        # else
        #     _tmp, _ordr, _missat, _needrev, intable=  _apply_by(x, idx, by[i], rev[i])
            # if _missat is :right it possible for fasttrack as long as lt = isless
        end
        if !_needrev && (eltype(_tmp) <: Union{Missing,Integer} || intable)
            # further check for fast integer sort
            n = length(_tmp)
            if n > 1
                minval::Integer = hp_minimum(_tmp)
                if ismissing(minval)
                    continue
                end
                maxval::Integer = hp_maximum(_tmp)
                (diff, o1) = sub_with_overflow(maxval, minval)
                (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
                if !o1 && !o2 && maxval < typemax(Int) && (rangelen < div(n,2)/(i ==1 ? Threads.nthreads() : 1))
                    for nt in 1:Threads.nthreads()
                        resize!(int_where[nt], rangelen + 2)
                    end
                    resize!(int_permcpy, length(idx))
                    copy!(int_permcpy, idx)
                    # if _missat == :left it means that we multiplied observations by -1 already and we should put missing at left
                    # note that -1 can not be applied to unsigned
                    if i == 1 && Threads.nthreads() > 1 && nrow(ds) > Threads.nthreads()
                        hp_ds_sort_int!(_tmp, idx, int_permcpy, int_where, rangelen, minval, _missat == :left, QuickSort, _ordr)
                    else
                        _sortperm_int!(idx, int_permcpy, _tmp, ranges, int_where, last_valid_range, _missat == :left, _ordr)
                    end
                else
                    if i == 1 && Threads.nthreads() > 1 && nrow(ds) > Threads.nthreads()
                        hp_ds_sort!(_tmp, idx, QuickSort, _ordr)
                    else
                        _sortperm_unstable!(idx, _tmp, ranges, last_valid_range, _ordr)
                    end
                end
            end
        else
            if i == 1 && Threads.nthreads() > 1 && nrow(ds) > Threads.nthreads()
                hp_ds_sort!(_tmp, idx, QuickSort, _ordr)
            else
                _sortperm_unstable!(idx, _tmp, ranges, last_valid_range, _ordr)
            end
        end
        # last_valid_range = _fill_starts!(ranges, _tmp, rangescpy, last_valid_range, _ordr, Val(T))
        last_valid_range = _fill_starts_v2!(ranges, inbits, _tmp, last_valid_range, _ordr, Val(T))
        # GC.enable(prv_gc)
        # GC.gc(false)
        last_valid_range == nrow(ds) && return (ranges, idx, last_valid_range)
    end
    return (ranges, idx, last_valid_range)
end

function _stablise_sort!(ranges, idx, last_valid_range)
    Threads.@threads for i in 1:last_valid_range
        rangestart = ranges[i]
        i == last_valid_range ? rangeend = length(idx) : rangeend = ranges[i+1] - 1
        if (rangeend - rangestart) == 0
            continue
        end
        sort!(idx, rangestart, rangeend, QuickSort, Forward)
    end
end

function _sortperm(ds::Dataset, cols::MultiColumnIndex, rev::Vector{Bool}; mapformats = true, stable = true)
    colsidx = index(ds)[cols]
    @assert length(colsidx) == length(rev) "`rev` argument must be the same as length of selected columns"
    by = Function[]
    if mapformats
        for j in 1:length(colsidx)
            push!(by, getformat(ds, colsidx[j]))
        end
    else
        for j in 1:length(colsidx)
            push!(by, identity)
        end
    end
    ranges, idx, last_valid_range = ds_sort_perm(ds, colsidx, by, rev, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
    if stable
        if length(idx) == last_valid_range
            return ranges, idx, last_valid_range
        else
            _stablise_sort!(ranges, idx, last_valid_range)
            return ranges, idx, last_valid_range
        end
    else
        return ranges, idx, last_valid_range
    end
end

function _sortperm(ds::Dataset, cols::MultiColumnIndex, rev::Bool = false; mapformats = true, stable = true)
    colsidx = index(ds)[cols]
    revs = repeat([rev], length(colsidx))
    _sortperm(ds, cols, revs; mapformats = mapformats, stable = stable)
end

_sortperm(ds::Dataset, col::ColumnIndex, rev::Bool = false; mapformats = true, stable = true) = _sortperm(ds, [col], rev; mapformats = mapformats, stable = stable)
