function _update_left_with_right!(x, y, ranges, checkmissing)
    Threads.@threads for i in 1:length(x)
        if length(ranges[i]) > 0
            if checkmissing && !ismissing(y[ranges[i].stop])
                x[i] = y[ranges[i].stop]
            elseif !checkmissing
                x[i] = y[ranges[i].stop]
            end
        end
    end
end

function _update!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, check = true, checkmissing = true) where T
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)

    sort!(dsr, oncols_right)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _fl = getformat(dsl, oncols_left[j])
        _fr = getformat(dsr, oncols_right[j])
        _find_ranges_for_join!(ranges, _columns(dsl)[oncols_left[j]], _columns(dsr)[oncols_right[j]], _fl, _fr)
    end

    for j in 1:length(right_cols)
        if haskey(index(dsl).lookup, _names(dsr)[right_cols[j]])
            left_cols_idx = index(dsl)[_names(dsr)[right_cols[j]]]
            TL = nonmissingtype(eltype(_columns(dsl)[left_cols_idx]))
            TR = nonmissingtype(eltype(_columns(dsr)[right_cols[j]]))
            if promote_type(TR, TL) <: TL
                _update_left_with_right!(_columns(dsl)[left_cols_idx], _columns(dsr)[right_cols[j]], ranges, checkmissing)
            end
        end
    end
    _modified(_attributes(dsl))
    dsl
end
