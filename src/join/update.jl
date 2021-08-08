function _update_left_with_right!(x, y, ranges, allowmissing, mode)
    Threads.@threads for i in 1:length(x)
        if length(ranges[i]) > 0
            if mode == :all
                if !allowmissing && !ismissing(y[ranges[i].stop])
                    x[i] = y[ranges[i].stop]
                elseif allowmissing
                    x[i] = y[ranges[i].stop]
                end
            elseif mode == :missing
                if ismissing(x[i])
                    if !allowmissing && !ismissing(y[ranges[i].stop])
                        x[i] = y[ranges[i].stop]
                    elseif allowmissing
                        x[i] = y[ranges[i].stop]
                    end
                end
            end
        end
    end
end

function _update!(dsl::Dataset, dsr::Dataset, ::Val{T}; onleft, onright, check = true, allowmissing = true, mode = :all, mapformats = [true, true], stable = false) where T
    isempty(dsl) && return dsl
    oncols_left = index(dsl)[onleft]
    oncols_right = index(dsr)[onright]
    right_cols = setdiff(1:length(index(dsr)), oncols_right)

    sort!(dsr, oncols_right, stable = stable)
    ranges = Vector{UnitRange{T}}(undef, nrow(dsl))
    fill!(ranges, 1:nrow(dsr))
    for j in 1:length(oncols_left)
        _change_refpool_find_range_for_join!(ranges, dsl, dsr, oncols_left, oncols_right, mapformats[1], mapformats[2], j)
    end

    for j in 1:length(right_cols)
        if haskey(index(dsl).lookup, _names(dsr)[right_cols[j]])
            left_cols_idx = index(dsl)[_names(dsr)[right_cols[j]]]
            TL = nonmissingtype(eltype(_columns(dsl)[left_cols_idx]))
            TR = nonmissingtype(eltype(_columns(dsr)[right_cols[j]]))
            if promote_type(TR, TL) <: TL
                _update_left_with_right!(_columns(dsl)[left_cols_idx], _columns(dsr)[right_cols[j]], ranges, allowmissing, mode)
            end
        end
    end
    _modified(_attributes(dsl))
    dsl
end
