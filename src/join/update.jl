function _update_left_with_right!(x, y, ranges, allowmissing, mode::F; threads = true) where F
    @_threadsfor threads for i in 1:length(x)
        if length(ranges[i]) > 0
            if mode(x[i])
                if !allowmissing && !ismissing(y[ranges[i].stop])
                    x[i] = y[ranges[i].stop]
                elseif allowmissing
                    x[i] = y[ranges[i].stop]
                end
            end
        end
    end
end

function _update!(dsl::Dataset, dsr::AbstractDataset, ::Val{T}; onleft, onright, check = true, allowmissing = true, mode = :all, mapformats = [true, true], stable = false, alg = HeapSort, accelerate = false, usehash = true, method = :sort, threads = true) where T
    isempty(dsl) && return dsl
    if method == :hash
        ranges, a, idx, minval, reps, sz, right_cols = _find_ranges_for_join_using_hash(dsl, dsr, onleft, onright, mapformats, true, Val(T); threads = threads)
    elseif method == :sort
        oncols_left = onleft
        oncols_right = onright
        right_cols = setdiff(1:length(index(dsr)), oncols_right)

        ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
        if usehash && length(oncols_left) == 1 && nrow(dsr)>1
            success, result = _update!_dict(dsl, dsr, ranges, oncols_left, oncols_right, right_cols, Val(T); mapformats = mapformats, allowmissing = allowmissing, mode = mode, threads = threads)
            if success
                return result
            end
        end
        idx, uniquemode = _find_permute_and_fill_range_for_join!(ranges, dsr, dsl, oncols_right, oncols_left, stable, alg, mapformats, accelerate, threads = threads)

        for j in 1:length(oncols_left)
            _change_refpool_find_range_for_join!(ranges, dsl, dsr, idx, oncols_left, oncols_right, mapformats[1], mapformats[2], j, threads = threads)
        end
    end

    if mode == :all
        f_mode = x->true
    elseif mode == :missing || mode == :missings
        f_mode = x->ismissing(x)
    else
        f_mode = x->mode(x)::Bool
    end
    for j in 1:length(right_cols)
        if haskey(index(dsl).lookup, _names(dsr)[right_cols[j]])
            left_cols_idx = index(dsl)[_names(dsr)[right_cols[j]]]
            TL = nonmissingtype(eltype(_columns(dsl)[left_cols_idx]))
            TR = nonmissingtype(eltype(_columns(dsr)[right_cols[j]]))
            if promote_type(TR, TL) <: TL
                _update_left_with_right!(_columns(dsl)[left_cols_idx], view(_columns(dsr)[right_cols[j]], idx), ranges, allowmissing, f_mode, threads = threads)
            end
        end
    end
    _modified(_attributes(dsl))
    dsl
end
