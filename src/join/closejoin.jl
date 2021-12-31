function _find_ranges_for_closeback!(ranges, x, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        curr_start = ranges[i].start
        if length(ranges[i]) == 0
            ranges[i] = 0:0
        else
            ranges[i] = 1:searchsortedlast_join(_fr, y, _fl(x[i])::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            if ranges[i].stop < curr_start
                ranges[i] = 0:curr_start
            end
        end
    end
end

function _find_ranges_for_closefor!(ranges, x, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        cur_stop = ranges[i].stop
        if length(ranges[i]) == 0
            ranges[i] = 0:0
        else
            ranges[i] = 1:searchsortedfirst_join(_fr, y, _fl(x[i])::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            if ranges[i].stop > cur_stop
                ranges[i] = 0:cur_stop
            end
        end
    end
end

function _find_ranges_for_closenearest!(ranges, x, y, _fl, _fr, ::Val{T1}, ::Val{T2}) where T1 where T2
    Threads.@threads for i in 1:length(x)
        cur_stop = ranges[i].stop
        curr_start = ranges[i].start
        if length(ranges[i]) == 0
            ranges[i] = 0:0
        else
            #TODO we don't need to search whole range for the second search
            fval = searchsortedfirst_join(_fr, y, _fl(x[i])::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))
            lval = searchsortedlast_join(_fr, y, _fl(x[i])::T1, ranges[i].start, ranges[i].stop, Base.Order.Forward, Val(T2))

            if fval > cur_stop
                ranges[i] = 0:lval
            elseif lval < curr_start
                ranges[i] = 0:fval
            else
                ranges[i] = min(fval, lval):max(fval, lval)
            end
        end
    end
end

_IST_(x,y)=true

function  _fill_right_cols_table_close!(_res, x, ranges, total, borderval, fill_val, direction; nn = false, rnn = nothing, lnn = nothing, tol = nothing, aem = _IST_ )
    if borderval == :nearest
        bordervalue = true
    elseif borderval == :none 
        bordervalue = missing
    elseif borderval == :missing 
        bordervalue = false
    end
    if tol === nothing
        if nn
            if ismissing(bordervalue)
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        r1 = rnn[ranges[i].stop]
                        if ranges[i].start == 0
                            _res[i] = missing
                        else
                            r2 = rnn[ranges[i].start]
                            if isless(abs(r2-lnn[i]), abs(r1-lnn[i]))
                                _res[i] = x[ranges[i].start]
                            else
                                _res[i] = x[ranges[i].stop]
                            end
                        end
                    end
                end
            else
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        r1 = rnn[ranges[i].stop]
                        if ranges[i].start == 0
                            _res[i] = x[ranges[i].stop]
                        else
                            r2 = rnn[ranges[i].start]
                            if isless(abs(r2-lnn[i]), abs(r1-lnn[i]))
                                _res[i] = x[ranges[i].start]
                            else
                                _res[i] = x[ranges[i].stop]
                            end
                        end
                    end
                end
            end
        else
            if ismissing(bordervalue)
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        if ranges[i].start == 0
                            _res[i] = missing
                        else
    
                            if direction == :backward
                                _res[i] = x[ranges[i].start]
                            else
                                _res[i] = x[ranges[i].stop]
                            end
                        end
                    end
                end
            else
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        _res[i] = x[ranges[i].stop]
                        if !bordervalue && ranges[i].start == 0
                            _res[i] = missing
                        end
                    end
                end
            end
        end
    else
        if nn
            if ismissing(bordervalue)
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        r1 = rnn[ranges[i].stop]
                        if ranges[i].start == 0
                            _res[i] = missing
                        else
                            r2 = rnn[ranges[i].start]
                            if isless(abs(r2-lnn[i]), abs(r1-lnn[i]))
                                if isless(abs(r2-lnn[i]), tol) && aem(r2, lnn[i])
                                    _res[i] = x[ranges[i].start]
                                else
                                    _res[i] = missing
                                end
                            else
                                if isless(abs(r1-lnn[i]), tol) && aem(r1, lnn[i])
                                    _res[i] = x[ranges[i].stop]
                                else
                                    _res[i] = missing
                                end
                            end
                        end
                    end
                end
            else
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        r1 = rnn[ranges[i].stop]
                        if ranges[i].start == 0
                            if isless(abs(r1-lnn[i]), tol) && aem(r1, lnn[i])
                                _res[i] = x[ranges[i].stop]
                            else
                                _res[i] = missing
                            end
                        else
                            r2 = rnn[ranges[i].start]
                            if isless(abs(r2-lnn[i]), abs(r1-lnn[i]))
                                if isless(abs(r2-lnn[i]), tol) && aem(r2, lnn[i])
                                    _res[i] = x[ranges[i].start]
                                else
                                    _res[i] = missing
                                end
                            else
                                if isless(abs(r1-lnn[i]), tol) && aem(r1, lnn[i])
                                    _res[i] = x[ranges[i].stop]
                                else
                                    _res[i] = missing
                                end
                            end
                        end
                    end
                end
            end
        else
            if ismissing(bordervalue)
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        if ranges[i].start == 0
                            _res[i] = missing
                        else
                            if direction == :backward
                                r1 = rnn[ranges[i].start]
                                if isless(abs(r1-lnn[i]), tol) && aem(r1, lnn[i])
                                    _res[i] = x[ranges[i].start]
                                else
                                    _res[i] = missing
                                end
                            else
                                r1 = rnn[ranges[i].stop]
                                if isless(abs(r1-lnn[i]), tol) && aem(r1, lnn[i])
                                    _res[i] = x[ranges[i].stop]
                                else
                                    _res[i] = missing
                                end
                            end
                        end                        
                    end
                end
            else
                Threads.@threads for i in 1:length(ranges)
                    if ranges[i] == 0:0
                        _res[i] = missing
                    else
                        r1 = rnn[ranges[i].stop]
                        if isless(abs(r1-lnn[i]), tol) && aem(r1, lnn[i])
                            _res[i] = x[ranges[i].stop]
                        else
                            _res[i] = missing
                        end
                        if !bordervalue && ranges[i].start == 0
                            _res[i] = missing
                        end
                    end
                end
            end
        end
    end

end

function _change_refpool_find_range_for_close!(ranges, dsl, dsr, r_perms, oncols_left, oncols_right, direction, lmf, rmf, j; nsfpaj = true)
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

    T1 = Core.Compiler.return_type(_fl, (eltype(var_l), ))

    if DataAPI.refpool(var_r) !== nothing && nsfpaj
        true && throw(ErrorException("we shouldn't end up here"))
    else
        T2 = Core.Compiler.return_type(_fr, (eltype(var_r), ))
        if direction == :backward
            _find_ranges_for_closeback!(ranges, var_l, view(var_r, r_perms), _fl, _fr, Val(T1), Val(T2))
        elseif direction == :forward
            _find_ranges_for_closefor!(ranges, var_l, view(var_r, r_perms), _fl, _fr, Val(T1), Val(T2))
        elseif direction == :nearest
            _find_ranges_for_closenearest!(ranges, var_l, view(var_r, r_perms), _fl, _fr, Val(T1), Val(T2))
        end
    end
end


# border = :nearest | :missing
function _join_closejoin(dsl, dsr::AbstractDataset, ::Val{T}; onleft, onright, makeunique = false, border = :nearest, mapformats = [true, true], stable = false, alg = HeapSort, accelerate = false, direction = :backward, inplace = false, tol = nothing,  allow_exact_match = true) where T
    isempty(dsl) && return copy(dsl)
    if !allow_exact_match
        #aem is the function to check allow_exact_match
        aem = !isequal
        # missing is greater than anything
        if tol == nothing
            tol = missing
        end
    else
        aem = _IST_
    end
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)
    if !makeunique && !isempty(intersect(_names(dsl), _names(dsr)[right_cols]))
        throw(ArgumentError("duplicate column names, pass `makeunique = true` to make them unique using a suffix automatically." ))
    end

    nsfpaj = true
    # if the column for close join is a PA we cannot use the fast path
    if DataAPI.refpool(_columns(dsr)[oncols_right[end]]) !== nothing
        nsfpaj = false
    end
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    idx, uniquemode = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate && length(oncols_right) > 1; nsfpaj = nsfpaj)

    for j in 1:(length(oncols_left) - 1)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j; nsfpaj = nsfpaj)
    end

    # if border = :none , we should :nearest direction
    _change_refpool_find_range_for_close!(ranges, dsl, dsr, idx, oncols_left, oncols_right, border == :none ? :nearest : direction, mapformats[1], mapformats[2], length(oncols_left); nsfpaj = nsfpaj)
    total_length = nrow(dsl)

    if inplace
        newds = dsl
    else
        res = []
        for j in 1:length(index(dsl))
            push!(res,  _columns(dsl)[j])
        end
        if dsl isa SubDataset
            newds = Dataset(res, copy(index(dsl)))
        else
            newds = Dataset(res, Index(copy(index(dsl).lookup), copy(index(dsl).names), copy(index(dsl).format)))
        end
    end

    for j in 1:length(right_cols)
        _res = allocatecol(_columns(dsr)[right_cols[j]], total_length)
        if DataAPI.refpool(_res) !== nothing
            fill_val = DataAPI.invrefpool(_res)[missing]
            _fill_right_cols_table_close!(_res.refs, view(DataAPI.refarray(_columns(dsr)[right_cols[j]]), idx), ranges, total_length, border, fill_val, direction; nn = direction == :nearest, rnn = view(_columns(dsr)[oncols_right[end]], idx), lnn = _columns(dsl)[oncols_left[end]], tol = tol, aem = aem)
        else
            _fill_right_cols_table_close!(_res, view(_columns(dsr)[right_cols[j]], idx), ranges, total_length, border, missing, direction; nn = direction == :nearest, rnn = view(_columns(dsr)[oncols_right[end]], idx), lnn = _columns(dsl)[oncols_left[end]], tol = tol, aem = aem)
        end
        push!(_columns(newds), _res)
        new_var_name = make_unique([_names(dsl); _names(dsr)[right_cols[j]]], makeunique = makeunique)[end]
        push!(index(newds), new_var_name)
        setformat!(newds, index(newds)[new_var_name], getformat(dsr, _names(dsr)[right_cols[j]]))
    end

    if inplace
        _modified(_attributes(newds))
    end
    newds

end
