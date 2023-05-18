function searchsortedfirst_join(_f, v::AbstractVector, x, lo::T, hi::T, o::Ordering, ::Val{T2})::keytype(v) where T<:Integer where T2
    u = T(1)
    lo = lo - u
    hi = hi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        fvm = _f(v[m])::T2
        if lt(o, fvm , x)
            lo = m
        else
            hi = m
        end
    end
    return hi
end

# index of the last value of vector a that is less than or equal to x;
# returns 0 if x is less than all values of v.
function searchsortedlast_join(_f, v::AbstractVector, x, lo::T, hi::T, o::Ordering, ::Val{T2})::keytype(v) where T<:Integer where T2
    u = T(1)
    lo = lo - u
    hi = hi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        fvm = _f(v[m])::T2
        if lt(o, x, fvm)
            hi = m
        else
            lo = m
        end
    end
    return lo
end

# returns the range of indices of v equal to x
# if v does not contain x, returns a 0-length range
# indicating the insertion point of x
function searchsorted_join(_f, v::AbstractVector, x, ilo::T, ihi::T, o::Ordering, ::Val{T2})::UnitRange{keytype(v)} where T<:Integer where T2
    u = T(1)
    lo = ilo - u
    hi = ihi + u
    @inbounds while lo < hi - u
        m = midpoint(lo, hi)
        fvm = _f(v[m])::T2
        if lt(o, fvm, x)
            lo = m
        elseif lt(o, x, fvm)
            hi = m
        else
            a = searchsortedfirst_join(_f, v, x, max(lo,ilo), m, o, Val(T2))
            b = searchsortedlast_join(_f, v, x, m, min(hi,ihi), o, Val(T2))
            return a : b
        end
    end
    return (lo + 1) : (hi - 1)
end

function _fill_range_for_accelerated_join!(ranges, starts, loc, x, f, sz, chunk; threads = true)
    loc_cumsum = cumsum(loc)
    @_threadsfor threads for i in 1:length(x)
        _index_ = hash(f(x[i])) % chunk + 1
        if loc[_index_]
            actual_start = loc_cumsum[_index_]
            st = starts[actual_start]
            actual_start == length(starts) ? en = sz : en = starts[actual_start+1]-1
            ranges[i] = st:en
        else
            ranges[i] = 1:0
        end
    end
end
# TODO how the hashing behave for Categorical Arrays?
function _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate, chunk = 2^10; nsfpaj=nsfpaj, threads = true)
    # nsfpaj has no value by default to make sure caller passes it
    # we use vector to represent nsfpaj, because we may override its value
    nsfpaj_in = nsfpaj[1]

    if isempty(dsr)
        idx = []
        fill!(ranges, 1:nrow(dsr))
        last_valid_range = -1
    else
         # check if data already sorted, if so it overrides accelerate 
        if _check_for_fast_sort(dsr, oncols_right, fill(false, length(oncols_right)), mapformats[2]; notsortpaforjoin = false, givenrange = nothing) == 0
            # if it is already sorted based on what we want we can saftly change nsfpaj to false
            nsfpaj[1] = false
            idx = 1:nrow(dsr)
            last_valid_range = _ngroups(dsr)
            fill!(ranges, 1:nrow(dsr))
        else

            if accelerate
                if mapformats[2]
                    _fr = getformat(dsr, oncols_right[1])
                else
                    _fr = identity
                end
                grng = _divide_for_fast_join(_columns(dsr)[oncols_right[1]], _fr, chunk; threads = threads)
                if mapformats[1]
                    _fl = getformat(dsl, oncols_left[1])
                else
                    _fl = identity
                end
                _fill_range_for_accelerated_join!(ranges, grng.starts, grng.starts_loc, _columns(dsl)[oncols_left[1]], _fl, nrow(dsr), chunk; threads = threads)
                if dsr isa SubDataset
                    starts, idx, last_valid_range =  _sortperm_v(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = nsfpaj_in, givenrange = grng, threads = threads)

                else
                    starts, idx, last_valid_range =  _sortperm(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = nsfpaj_in, givenrange = grng, threads = threads)
                end
            else
                if dsr isa SubDataset
                    starts, idx, last_valid_range =  _sortperm_v(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = nsfpaj_in, threads = threads)
                else
                    starts, idx, last_valid_range =  _sortperm(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = nsfpaj_in, threads = threads)
                end
                fill!(ranges, 1:nrow(dsr))
            end
        end
    end
    idx, last_valid_range == length(idx)
end

function _sort_for_join_after_hash(dsr, oncols_right, stable, alg, mapformats, nsfpaj, grng; threads = true)
    nsfpaj_in = nsfpaj[1]
    if dsr isa SubDataset
        starts, idx, last_valid_range =  _sortperm_v(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = nsfpaj_in, givenrange = grng, threads = threads)
    else
        starts, idx, last_valid_range =  _sortperm(dsr, oncols_right, stable = stable, a = alg, mapformats = mapformats[2], notsortpaforjoin = nsfpaj_in, givenrange = grng, threads = threads)
    end
end

# find count of each groups id
function _find_counts_for_join(groups, ngroups)
   res = zeros(Int, ngroups)
   for i in 1:length(groups)
       res[groups[i]] += 1
   end
   res
end

function _find_range_for_join!(ranges, ldata, gslots, reps, where, minval, sz; threads = true)
    @_threadsfor threads for i in 1:length(ldata)
        ranges[i] = _query_dictionary_for_join_int(identity, ldata[i], gslots, reps, where, minval, sz)
    end
end

Base.@propagate_inbounds function _fill_val_join!(x, r, val)
    @simd for i in r
        x[i] = val
    end
end

function _fill_val_join!(x, r2, val, inbits, r)
    cnt = 1
    lo = r2.start
    for i in r
        if inbits[i]
            x[cnt+lo-1] = val
            cnt += 1
        end
    end
end
# F1 and F2 are here for type stability when threads = false
function _find_ranges_for_join!(ranges, x, y, _fl::F1, _fr::F2, ::Val{T1}, ::Val{T2}; type = :both, threads = true) where {T1, T2, F1, F2}
    if type == :both
        @_threadsfor threads for i in 1:length(x)
            ranges[i] = searchsorted_join(_fr, y, DataAPI.unwrap(_fl(x[i]))::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
        end
    # TODO having another elseif branch is better for performance.
    elseif type == :left || type == :rightstrict
        @_threadsfor threads for i in 1:length(x)
             _flx = _fl(DataAPI.unwrap(x[i]))::T1
            hi = searchsortedlast_join(_fr, y, _flx, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            lo = ranges[i].start
            if type === :rightstrict
                ranges[i] = hi+1:ranges[i].stop
            else
                ranges[i] = lo:hi
            end
        end
    elseif type == :right || type == :leftstrict
        @_threadsfor threads for i in 1:length(x)
            _flx = _fl(DataAPI.unwrap(x[i]))::T1
            lo = searchsortedfirst_join(_fr, y, _flx, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            hi = ranges[i].stop
            if type === :leftstrict
                    ranges[i] = ranges[i].start:lo-1
            else
                ranges[i] = lo:hi
            end
        end
    end
end

function _find_ranges_for_join_pa!(ranges, x, invpool, y, _fl::F1, _fr::F2, ::Val{T1}, ::Val{T2}; type = :both, threads = true) where {T1, T2, F1, F2}
    if type == :both
        @_threadsfor threads for i in 1:length(x)
            revmap_paval_ref = get(invpool, DataAPI.unwrap(_fl(x[i]))::T1, missing)
            if ismissing(revmap_paval_ref)
                ranges[i] = 1:0
            else
                #_fr is identity
                ranges[i] = searchsorted_join(identity, y, revmap_paval_ref, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            end
        end
    elseif type == :left || type == :rightstrict
        @_threadsfor threads for i in 1:length(x)
            revmap_paval_ref = get(invpool, _fl(DataAPI.unwrap(x[i]))::T1, missing)
            if ismissing(revmap_paval_ref)
                ranges[i] = 1:0
            else
                #_fr is identity
                hi = searchsoredlast_join(identity, y, revmap_paval_ref, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
                lo = ranges[i].start
                if type === :rightstrict
                    ranges[i] = hi+1:ranges[i].stop
                else
                    ranges[i] = lo:hi
                end
            end
        end
    elseif type == :right || type == :leftstrict
        @_threadsfor threads for i in 1:length(x)
            revmap_paval_ref = get(invpool, _fl(DataAPI.unwrap(x[i]))::T1, missing)
            if ismissing(revmap_paval_ref)
                ranges[i] = 1:0
            else
                #_fr is identity
                lo = searchsortedfirst_join(identity, y, revmap_paval_ref, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
                hi = ranges[i].stop
                if type === :leftstrict
                    ranges[i] = ranges[i].start:lo-1
                else
                    range[i] = lo:hi
                end
            end
        end
    end

end


function _fill_oncols_left_table_left!(_res, x, ranges, en, total, fill_val; threads = true)
    @_threadsfor threads for i in 1:length(x)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        _fill_val_join!(_res, lo:hi, x[i])
    end
    @_threadsfor threads for i in en[length(x)]+1:total
        _res[i] = fill_val
    end
end

function _fill_oncols_left_table_inner!(_res, x, ranges, en, total; inbits = nothing, en2 = nothing, threads = true)
    if inbits === nothing
        @_threadsfor threads for i in 1:length(x)
            length(ranges[i]) == 0 && continue
            i == 1 ? lo = 1 : lo = en[i - 1] + 1
            hi = en[i]
            _fill_val_join!(_res, lo:hi, x[i])
        end
    else
        @_threadsfor threads for i in 1:length(x)
            length(ranges[i]) == 0 && continue
            if i == 1
                lo = 1
                lo2 = 1
            else
                lo = en[i - 1] + 1
                lo2 = en2[i-1] + 1
            end
            hi = en[i]
            # @show sum(view(inbits, lo:hi))
            # sum(view(inbits, lo:hi)) == 0 && continue
            hi2 = en2[i]
            _fill_val_join!(_res, lo2:hi2, x[i], inbits, lo:hi)
        end
    end
end

function _fill_oncols_left_table_anti!(_res, x, ranges, en, total; threads = true)
    @_threadsfor threads for i in 1:length(x)
        length(ranges[i]) != 0 && continue
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        _fill_val_join!(_res, lo:hi, x[i])
    end
end

function _fill_right_cols_table_left!(_res, x, ranges, en, total, fill_val; threads = true)
    @_threadsfor threads for i in 1:length(ranges)
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        length(ranges[i]) == 0 ? _fill_val_join!(_res, lo:hi, fill_val) : copyto!(_res, lo, x, ranges[i].start, length(ranges[i]))
    end
end

function _fill_right_col_range!(_res, r2, x, ranges, inbits, r)
    cnt = 1
    cnt_r = 1
    lo = r2.start
    for i in r
        if inbits[i]
            _res[lo+cnt-1] = x[ranges[cnt_r]]
            cnt += 1
        end
        cnt_r += 1
    end
end

function _fill_right_cols_table_inner!(_res, x, ranges, en, total; inbits = nothing, en2 = nothing, threads = true)
    if inbits === nothing
        @_threadsfor threads for i in 1:length(ranges)
            length(ranges[i]) == 0 && continue
            i == 1 ? lo = 1 : lo = en[i - 1] + 1
            hi = en[i]
            copyto!(_res, lo, x, ranges[i].start, length(ranges[i]))
        end
    else
        @_threadsfor threads for i in 1:length(ranges)
            length(ranges[i]) == 0 && continue
            if i == 1
                lo = 1
                lo2 = 1
            else
                lo = en[i - 1] + 1
                lo2 = en2[i-1] + 1
            end
            hi = en[i]

            hi2 = en2[i]
            _fill_right_col_range!(_res, lo2:hi2, x, ranges[i], inbits, lo:hi)
        end
    end
end

function _create_multiple_match_col_left(ranges, total_length)
    res = allocatecol(Bool, total_length)
    cnt = 0
    for i in 1:length(ranges)
        if length(ranges[i]) == 0
            cnt += 1
            res[cnt] = false
        else
            if length(ranges[i]) == 1
                cnt += 1
                res[cnt] = false
            else
                for j in ranges[i]
                    cnt += 1
                    res[cnt] = true
                end
            end
        end
    end
    res
end
function _create_multiple_match_col_inner(ranges, en, total_length)
    res = allocatecol(Bool, total_length)
    cnt = 0
    if en === nothing
        for i in 1:length(ranges)
            if length(ranges[i]) == 0
                nothing
            else
                if length(ranges[i]) == 1
                    cnt += 1
                    res[cnt] = false
                else
                    for j in ranges[i]
                        cnt += 1
                        res[cnt] = true
                    end
                end
            end
        end
    else
        for i in 1:length(ranges)
            if i == 1
                lo = 1
            else
                lo = en[i - 1] + 1
            end
            hi = en[i]
            if length(lo:hi) == 0
                nothing
            elseif length(lo:hi) == 1
                cnt += 1
                res[cnt] = false
            else
                for j in lo:hi
                    cnt += 1
                    res[cnt] = true
                end
            end
        end
    end

    res
end



ISLE(x, ::Missing) = true
ISLE(x, y) = (x <= y)
ISLE(::Missing, y) = false
ISLE(::Missing, ::Missing) = false


function _mark_lt_part!(inbits, x_l, x_r, _fl::F1, _fr::F2, ranges, r_perms, en, ::Val{T}; strict = false, threads = true) where {T, F1, F2}
    revised_ends = zeros(T, length(en))
    @_threadsfor threads for i in 1:length(ranges)
        if length(ranges[i]) == 0
            if i !== 1
                revised_ends[i] = 0
            end
            continue
        end
        i == 1 ? lo = 1 : lo = en[i - 1] + 1
        hi = en[i]
        total = 0
        cnt = 1
        for j in ranges[i]
            if strict
                inbits[lo + cnt - 1] = isless(_fl(x_l[i]), _fr(x_r[r_perms[j]]))
            else
                inbits[lo + cnt - 1] = ISLE(_fl(x_l[i]), _fr(x_r[r_perms[j]]))
            end
            total += inbits[lo + cnt - 1]
            cnt += 1
        end
        revised_ends[i] = total
        if total == 0
            ranges[i] = 1:0
        end
    end
    our_cumsum!(revised_ends)
end

function _change_refpool_find_range_for_join!(ranges, dsl, dsr, r_perms, oncols_left, oncols_right, lmf, rmf, j; type = :both, nsfpaj=nsfpaj, threads = true)
    nsfpaj_in = nsfpaj[1]
    var_l = _columns(dsl)[oncols_left[j]]
    var_r = _columns(dsr)[oncols_right[j]]
    l_idx = oncols_left[j]
    r_idx = oncols_right[j]
    if lmf
        _fl = getformat(dsl, l_idx)
    else
        _fl = identity
    end
    if rmf
        _fr = getformat(dsr, r_idx)
    else
        _fr = identity
    end

    T1 = Core.Compiler.return_type(DataAPI.unwrap∘_fl, Tuple{eltype(var_l)})

    if DataAPI.refpool(var_r) !== nothing && nsfpaj_in
        # sort taken care for refs ordering of modified values, but we still need to change refs
        if _fr == identity
            var_r_cpy = var_r
        else
            var_r_cpy = map(_fr, var_r)
        end
        T2 = eltype(DataAPI.refarray(var_r_cpy))
        # now _fr must be identity
        _fr = identity
        # we should use invpool of right column
        _find_ranges_for_join_pa!(ranges, var_l, DataAPI.invrefpool(var_r_cpy), view(DataAPI.refarray(var_r_cpy), r_perms), _fl, _fr, Val(T1), Val(T2); type = type, threads = threads)
    else
        T2 = Core.Compiler.return_type(_fr, Tuple{eltype(var_r)})
        _find_ranges_for_join!(ranges, var_l, view(var_r, r_perms), _fl, _fr, Val(T1), Val(T2); type = type, threads = threads)
    end
end



function _join_left(dsl, dsr, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false, method = :sort, threads = true, multiple_match::Bool = false, multiple_match_name = :multiple, obs_id = [false, false], obs_id_name = :obs_id) where T
    isempty(dsl) && return copy(dsl)
    nsfpaj = [true]
    if method == :hash
        ranges, a, idx, minval, reps, sz, right_cols = _find_ranges_for_join_using_hash(dsl, dsr, onleft, onright, mapformats, makeunique, Val(T); threads = threads)
    elseif method == :sort
        oncols_left = onleft
        oncols_right = onright
        right_cols = setdiff(1:length(index(dsr)), oncols_right)
        if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
            throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
        end

        ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
        if length(oncols_left) == 1 && nrow(dsr)>1
            success, result = _join_left_dict(dsl, dsr, ranges, oncols_left, oncols_right, right_cols, Val(T); makeunique = makeunique, mapformats = mapformats, check = check, threads = threads, multiple_match = multiple_match, multiple_match_name = multiple_match_name, obs_id = obs_id, obs_id_name = obs_id_name)
            if success
                return result
            end
        end
        idx, uniquemode = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate;nsfpaj = nsfpaj, threads = threads)

        for j in 1:length(oncols_left)
            _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j; nsfpaj = nsfpaj, threads = threads)
        end
    end
    new_ends = map(x -> max(1, length(x)), ranges)
    our_cumsum!(new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end

    if multiple_match
        multiple_match_col = _create_multiple_match_col_left(ranges, total_length)
    end

    res = []
    for j in 1:length(index(dsl))
        addmissing = false
        _res = allocatecol(_columns(dsl)[j], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            # fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_oncols_left_table_left!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length, missing;  threads = threads)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing; threads = threads)
        end
        push!(res, _res)

    end
    if dsl isa SubDataset
        newds = Dataset(res, copy(index(dsl)), copycols = false)
    else
        newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
    end


    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, view(DataAPI.refarray(_columns(dsr)[right_cols[j]]), idx), ranges, new_ends, total_length, fill_val; threads = threads)
        else
            _fill_right_cols_table_left!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length, missing; threads = threads)
        end
        push!(_columns(newds), _res)

        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    if multiple_match
        insertcols!(newds, ncol(newds)+1, multiple_match_name => multiple_match_col, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[1]
        obs_id_name1 = Symbol(obs_id_name, "_left")
        obs_id_left = allocatecol(nrow(dsl) < typemax(Int32) ? Int32 : Int64, total_length)
        _fill_oncols_left_table_left!(obs_id_left, 1:nrow(dsl), ranges, new_ends, total_length, missing; threads = threads)
        insertcols!(newds, ncol(newds)+1, obs_id_name1 => obs_id_left, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[2]
        obs_id_name2 = Symbol(obs_id_name, "_right")
        obs_id_right = allocatecol(T, total_length)
        _fill_right_cols_table_left!(obs_id_right, idx, ranges, new_ends, total_length, missing, threads = threads)
        insertcols!(newds, ncol(newds)+1, obs_id_name2 => obs_id_right, unsupported_copy_cols = false, makeunique = makeunique)
    end
    newds

end

function _join_left!(dsl::Dataset, dsr::AbstractDataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false, method = :sort, threads = true, multiple_match = false, multiple_match_name = :multiple, obs_id = [false, false], obs_id_name = :obs_id) where T
    isempty(dsl) && return dsl
    nsfpaj = [true]
    if method == :hash
        ranges, a, idx, minval, reps, sz, right_cols = _find_ranges_for_join_using_hash(dsl, dsr, onleft, onright, mapformats, makeunique, Val(T); threads = threads)
    elseif method == :sort
        oncols_left = onleft
        oncols_right = onright
        right_cols = setdiff(1:length(index(dsr)), oncols_right)
        if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
            throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
        end
        ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
        if length(oncols_left) == 1 && nrow(dsr)>1
            success, result = _join_left!_dict(dsl, dsr, ranges, oncols_left, oncols_right, right_cols, Val(T); makeunique = makeunique, mapformats = mapformats, check = check, threads = threads, multiple_match = multiple_match, multiple_match_name = multiple_match_name, obs_id = obs_id, obs_id_name = obs_id_name)
            if success
                return result
            end
        end
        idx, uniquemode = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate, nsfpaj = nsfpaj, threads = threads)
        for j in 1:length(oncols_left)
            _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j, nsfpaj = nsfpaj, threads = threads)
        end
    end
    if !all(x->length(x) <= 1, ranges)
        throw(ArgumentError("`leftjoin!` can only be used when each observation in left data set matches at most one observation from right data set"))
    end

    new_ends = map(x -> max(1, length(x)), ranges)
    our_cumsum!(new_ends)
    total_length = new_ends[end]

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end

    if multiple_match
        multiple_match_col = _create_multiple_match_col_left(ranges, total_length)
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, view(DataAPI.refarray(_columns(dsr)[right_cols[j]]), idx), ranges, new_ends, total_length, fill_val, threads = threads)
        else
            _fill_right_cols_table_left!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length, missing, threads = threads)
        end
        push!(_columns(dsl), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(dsl), new_var_name)
        setformat!(dsl, index(dsl)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    if multiple_match
        insertcols!(dsl, ncol(dsl)+1, multiple_match_name => multiple_match_col, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[1]
        obs_id_name1 = Symbol(obs_id_name, "_left")
        obs_id_left = allocatecol(nrow(dsl) < typemax(Int32) ? Int32 : Int64, total_length)
        _fill_oncols_left_table_left!(obs_id_left, 1:nrow(dsl), ranges, new_ends, total_length, missing; threads = threads)
        insertcols!(dsl, ncol(dsl)+1, obs_id_name1 => obs_id_left, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[2]
        obs_id_name2 = Symbol(obs_id_name, "_right")
        obs_id_right = allocatecol(T, total_length)
        _fill_right_cols_table_left!(obs_id_right, idx, ranges, new_ends, total_length, missing, threads = threads)
        insertcols!(dsl, ncol(dsl)+1, obs_id_name2 => obs_id_right, unsupported_copy_cols = false, makeunique = makeunique)
    end

    _modified(_attributes(dsl))
    dsl
end

function _join_inner(dsl, dsr::AbstractDataset, ::Val{T}; onleft, onright, onright_range = nothing , makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false, droprangecols = true, strict_inequality = [false, false], method = :sort, threads = true, onlyreturnrange = false, multiple_match = false, multiple_match_name = :multiple, obs_id = [false, false], obs_id_name = :obs_id) where T
    (isempty(dsl) || isempty(dsr)) && throw(ArgumentError("in `innerjoin` both left and right tables must be non-empty"))
    oncols_left = onleft
    oncols_right = onright
    type = :both
    right_range_cols = Int[]
    if onright_range !== nothing
        left_range_col = oncols_left[end]

        right_range_cols = index(dsr)[filter!(!isequal(nothing), collect(onright_range))]
        if droprangecols
            right_cols = setdiff(1:length(index(dsr)), [oncols_right; right_range_cols])
        else
            right_cols = setdiff(1:length(index(dsr)), oncols_right)
        end

        oncols_right = [oncols_right; first(right_range_cols)]
        if onright_range[1] !== nothing
            if strict_inequality[1]
                type = :leftstrict
            else
                type = :left
            end
        else
            if strict_inequality[2]
                type = :rightstrict
            else
                type = :right
            end
        end
    else
        right_cols = setdiff(1:length(index(dsr)), oncols_right)
    end
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end

    nsfpaj = [true]
    # if the columns for inequality like join are PA we cannot use the fast path
    if type != :both
        if any(i-> DataAPI.refpool(_columns(dsr)[i]) !== nothing, right_range_cols)
            nsfpaj = [false]
        end
    end
    # if (onright_range === nothing || length(onleft) > 1) is false, then we have inequality kind join with no exact match join
    if method == :hash && (onright_range === nothing || length(onleft) > 1)
        if onright_range !== nothing
            ranges, a, idx, minval, reps, sz, right_cols_2 = _find_ranges_for_join_using_hash(dsl, dsr, onleft[1:end-1], oncols_right[1:end-1], mapformats, true, Val(T); threads = threads)
            filter!(!=(0), reps)
            pushfirst!(reps, 1)
            our_cumsum!(reps)
            pop!(reps)
            grng = GIVENRANGE(idx, reps, Int[], length(reps))
            starts, idx, last_valid_range = _sort_for_join_after_hash(dsr, right_range_cols[1], stable, alg, mapformats, nsfpaj, grng; threads = threads)
            _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], length(oncols_left); type = type, nsfpaj = nsfpaj, threads = threads)
        else
            ranges, a, idx, minval, reps, sz, right_cols = _find_ranges_for_join_using_hash(dsl, dsr, onleft, onright, mapformats, makeunique, Val(T); threads = threads)
        end
    else
        ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
        if length(oncols_left) == 1 && type == :both && nrow(dsr)>1
            success, result =  _join_inner_dict(dsl, dsr, ranges, oncols_left, oncols_right, right_cols, Val(T); makeunique = makeunique, mapformats = mapformats, check = check, threads = threads, multiple_match = multiple_match, multiple_match_name = multiple_match_name, obs_id = obs_id, obs_id_name = obs_id_name)
            if success
                return result
            end
        end
        idx, uniquemode = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate && (onright_range === nothing || length(oncols_right)>1); nsfpaj = nsfpaj, threads = threads)

        for j in 1:length(oncols_left)-1
            _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j; nsfpaj = nsfpaj, threads = threads)
        end
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], length(oncols_left); type = type, nsfpaj = nsfpaj, threads = threads)
    end



    new_ends = map(length, ranges)
    our_cumsum!(new_ends)
    total_length = new_ends[end]

    inbits = nothing
    revised_ends = nothing
    if length(right_range_cols) == 2
        inbits = zeros(Bool, total_length)
        # TODO any optimisation is needed for pa?
        _fl = identity
        _fr = identity
        if mapformats[1]
            _fl = getformat(dsl, left_range_col)
        end
        if mapformats[2]
            _fr = getformat(dsr, right_range_cols[2])
        end
        revised_ends = _mark_lt_part!(inbits, _columns(dsl)[left_range_col], _columns(dsr)[right_range_cols[2]], _fl, _fr, ranges, idx, new_ends, total_length < typemax(Int32) ? Val(Int32) : Val(Int64); strict = strict_inequality[2], threads = threads)
    end
    if length(right_range_cols) == 2
        total_length = sum(inbits)
    end

    if onlyreturnrange
        return ranges
    end
    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end
    if multiple_match
        multiple_match_col = _create_multiple_match_col_inner(ranges, revised_ends, total_length)
    end

    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            _fill_oncols_left_table_inner!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length; inbits = inbits, en2 = revised_ends, threads = threads)
        else
            _fill_oncols_left_table_inner!(_res, _columns(dsl)[j], ranges, new_ends, total_length; inbits = inbits, en2 = revised_ends, threads = threads)
        end
        push!(res, _res)
    end
    if dsl isa SubDataset
        newds = Dataset(res, copy(index(dsl)), copycols = false)
    else
        newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length, addmissing = false)
        if DataAPI.refpool(_res) !== nothing
            _fill_right_cols_table_inner!(_res.refs, view(DataAPI.refarray(_columns(dsr)[right_cols[j]]), idx), ranges, new_ends, total_length; inbits = inbits, en2 = revised_ends, threads = threads)
        else
            _fill_right_cols_table_inner!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length; inbits = inbits, en2 = revised_ends, threads = threads)
        end
        push!(_columns(newds), _res)

        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    if multiple_match
        insertcols!(newds, ncol(newds)+1, multiple_match_name => multiple_match_col, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[1]
        obs_id_name1 = Symbol(obs_id_name, "_left")
        obs_id_left = allocatecol(nrow(dsl) < typemax(Int32) ? Int32 : Int64, total_length)
        _fill_oncols_left_table_inner!(obs_id_left, 1:nrow(dsl), ranges, new_ends, total_length; inbits = inbits, en2 = revised_ends, threads = threads)
        insertcols!(newds, ncol(newds)+1, obs_id_name1 => obs_id_left, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[2]
        obs_id_name2 = Symbol(obs_id_name, "_right")
        obs_id_right = allocatecol(T, total_length)
        _fill_right_cols_table_inner!(obs_id_right, idx, ranges, new_ends, total_length; inbits = inbits, en2 = revised_ends, threads = threads)
        insertcols!(newds, ncol(newds)+1, obs_id_name2 => obs_id_right, unsupported_copy_cols = false, makeunique = makeunique)
    end
    newds

end

function _in(dsl::AbstractDataset, dsr::AbstractDataset, ::Val{T}; onleft, onright, mapformats = [true, true], stable = false, alg = HeapSort, accelerate = false, threads = true) where T
    isempty(dsl) && return Bool[]
    oncols_left = onleft
    oncols_right = onright
    nsfpaj = [true]
    # use Set when there is only one column in `on`
    if length(oncols_right) == 1
        if mapformats[1]
            _fl = getformat(dsl, oncols_left[1])
        else
            _fl = identity
        end
        if mapformats[2]
            _fr = getformat(dsr, oncols_right[1])
        else
            _fr = identity
        end
        return _in_use_Set(_columns(dsl)[oncols_left[1]], _columns(dsr)[oncols_right[1]], _fl, _fr, threads = threads)
    end
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    idx, uniquemode = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate, nsfpaj = nsfpaj, threads = threads)
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j, nsfpaj = nsfpaj, threads = threads)
    end
    map(x -> length(x) == 0 ? false : true, ranges)
end


function _find_right_not_in_left(ranges, n, idx)
    res = trues(n)
    for i in 1:length(ranges)
        view(res, ranges[i]) .= false
    end
    findall(res)
end

function _fill_oncols_left_table_left_outer!(res, x, notinleft, en, total)
    # TODO when x is a pooled array, we cannot use Threads.@threads (maybe we should create pool first and then use Threads)
    for i in en[end]+1:total
        val = x[notinleft[i - en[end]]]
        res[i] = val
    end
end

function _fill_source_for_outer!(res, ranges, notinleft, lval, rval, en, total)
    cnt = 0
    for i in 1:length(ranges)
        if length(ranges[i]) == 0
            cnt += 1
            res[cnt] = lval
        else
            cnt += length(ranges[i])
        end
    end
    for i in en[end]+1:total
        res[i] = rval
    end
end



function _create_source_for_outer(ranges, notinleft, total_length, en)
    res = allowmissing(PooledArray(["left", "right", "both"]))
    resize!(res.refs, total_length)
    fill!(res.refs, get(res.invpool, "both", missing))
    _fill_source_for_outer!(res.refs, ranges, notinleft, get(res.invpool, "left", missing), get(res.invpool, "right", missing), en, total_length)
    res
end

function _create_multiple_match_col_outer(ranges, notinleft, total_length, new_ends)
    res = _missings(Bool, total_length)
    cnt = 0
    for i in 1:length(ranges)
        if length(ranges[i]) == 0
            cnt += 1
            res[cnt] = false
        else
            if length(ranges[i]) == 1
                cnt += 1
                res[cnt] = false
            else
                for j in ranges[i]
                    cnt += 1
                    res[cnt] = true
                end
            end
        end
    end
    res
end

function _join_outer(dsl, dsr::AbstractDataset, ::Val{T}; onleft, onright, makeunique = false, mapformats = [true, true], stable = false, alg = HeapSort, check = true, accelerate = false, method = :sort, threads = true, source::Bool = false, source_col_name = :source, multiple_match = false, multiple_match_name = :multiple, obs_id = [false, false], obs_id_name = :obs_id) where T
    (isempty(dsl) || isempty(dsr)) && throw(ArgumentError("in `outerjoin` both left and right tables must be non-empty"))
    oncols_left = onleft
    oncols_right = onright
    nsfpaj = [true]
    if method == :hash
        ranges, a, idx, minval, reps, sz, right_cols = _find_ranges_for_join_using_hash(dsl, dsr, onleft, onright, mapformats, makeunique, Val(T); threads = threads)
    elseif method == :sort
        right_cols = setdiff(1:length(index(dsr)), oncols_right)
        if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
            throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
        end
        ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
        if length(oncols_left) == 1 && nrow(dsr)>1
            success, result = _join_outer_dict(dsl, dsr, ranges, oncols_left, oncols_right, oncols_left, oncols_right, right_cols, Val(T); makeunique = makeunique, mapformats = mapformats, check = check, threads = threads, source = source, source_col_name = source_col_name, multiple_match = multiple_match, multiple_match_name = multiple_match_name, obs_id = obs_id, obs_id_name = obs_id_name)
            if success
                return result
            end
        end
        idx, uniquemode = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate, nsfpaj = nsfpaj, threads = threads)
        for j in 1:length(oncols_left)
            _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j, nsfpaj = nsfpaj, threads = threads)
        end
    end
    new_ends = map(x -> max(1, length(x)), ranges)
    notinleft = _find_right_not_in_left(ranges, nrow(dsr), idx)
    our_cumsum!(new_ends)
    total_length = new_ends[end] + length(notinleft)

    if check
        @assert total_length < 10*nrow(dsl) "the output data set will be very large ($(total_length)×$(ncol(dsl)+length(right_cols))) compared to the left data set size ($(nrow(dsl))×$(ncol(dsl))), make sure that the `on` keyword is selected properly, alternatively, pass `check = false` to ignore this error."
    end

    if source
        source_col = _create_source_for_outer(ranges, notinleft, total_length, new_ends)
    end
    if multiple_match
        multiple_match_col = _create_multiple_match_col_outer(ranges, notinleft, total_length, new_ends)
    end

    res = []
    for j in 1:length(index(dsl))
        _res = allocatecol(_columns(dsl)[j], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_oncols_left_table_left!(_res.refs, DataAPI.refarray(_columns(dsl)[j]), ranges, new_ends, total_length, fill_val, threads = threads)
        else
            _fill_oncols_left_table_left!(_res, _columns(dsl)[j], ranges, new_ends, total_length, missing, threads = threads)
        end
        push!(res, _res)
    end
    for j in 1:length(oncols_left)
        _fill_oncols_left_table_left_outer!(res[oncols_left[j]], view(_columns(dsr)[oncols_right[j]], idx), notinleft, new_ends, total_length)
    end
    if dsl isa SubDataset
        newds = Dataset(res, copy(index(dsl)), copycols = false)
    else
        newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)), copycols = false)
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_left!(_res.refs, view(DataAPI.refarray(_columns(dsr)[right_cols[j]]), idx), ranges, new_ends, total_length, fill_val, threads = threads)
            _fill_oncols_left_table_left_outer!(_res.refs, view(DataAPI.refarray(_columns(dsr)[right_cols[j]]), idx), notinleft, new_ends, total_length)
        else
            _fill_right_cols_table_left!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, new_ends, total_length, missing, threads = threads)
            _fill_oncols_left_table_left_outer!(_res, view(_columns(dsr)[right_cols[j]], idx), notinleft, new_ends, total_length)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end
    if source
        insertcols!(newds, ncol(newds)+1, source_col_name => source_col, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if multiple_match
        insertcols!(newds, ncol(newds)+1, multiple_match_name => multiple_match_col, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[1]
        # Note that the name convention of obs_id_name1 and name2 are used in other places
        obs_id_name1 = Symbol(obs_id_name, "_left")
        obs_id_left = allocatecol(nrow(dsl) < typemax(Int32) ? Int32 : Int64, total_length)
        _fill_oncols_left_table_left!(obs_id_left, 1:nrow(dsl), ranges, new_ends, total_length, missing, threads = threads)
        insertcols!(newds, ncol(newds)+1, obs_id_name1 => obs_id_left, unsupported_copy_cols = false, makeunique = makeunique)
    end
    if obs_id[2]
        obs_id_name2 = Symbol(obs_id_name, "_right")
        obs_id_right = allocatecol(T, total_length)
        _fill_right_cols_table_left!(obs_id_right, idx, ranges, new_ends, total_length, missing, threads = threads)
        _fill_oncols_left_table_left_outer!(obs_id_right, idx, notinleft, new_ends, total_length)
        insertcols!(newds, ncol(newds)+1, obs_id_name2 => obs_id_right, unsupported_copy_cols = false, makeunique = makeunique)
    end
    newds

end
