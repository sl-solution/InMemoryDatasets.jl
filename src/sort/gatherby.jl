using .Base: sub_with_overflow, add_with_overflow, mul_with_overflow

# FIXME it needs more work, e.g it needs to handle missing values
function _gatherby(ds, cols, ::Val{T}; mapformats = false) where T
    colidx = index(ds)[cols]
    n = nrow(ds)
    flag = false
    _max_level = nrow(ds)
    prev_max_group = UInt(1)
    prev_groups = ones(UInt, nrow(ds))
    groups = Vector{T}(undef, nrow(ds))
    seen_non_int = false

    rhashes = Vector{UInt}(undef, 1)
    sz = max(1 + ((5 * _max_level) >> 2), 16)
    sz = 1 << (8 * sizeof(sz) - leading_zeros(sz - 1))
    @assert 4 * sz >= 5 * _max_level
    gslots = Vector{T}(undef, 1)

    for j in 1:length(colidx)
        _f = identity
        if mapformats
            _f = getformat(ds, colidx[j])
        end
        _f = _date_value∘_f

        CT = Core.Compiler.return_type(_f, (eltype(_columns(ds)[colidx[j]]),))
        # assuming format is not changing the number of levels in pooled array
        if DataAPI.refpool(_columns(ds)[colidx[j]]) !== nothing
            v = DataAPI.refarray(_columns(ds)[colidx[j]])
            minval = hp_minimum(_f, v)
            if ismissing(minval)
                continue
            end
            maxval = hp_maximum(_f, v)
            (diff, o1) = sub_with_overflow(maxval, minval)
            (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
            (outmult, o3) = mul_with_overflow(Int(rangelen), Int(prev_max_group))
            if !o1 && !o2 && !o3 && maxval < typemax(Int) && (rangelen < div(n,2)) && prev_max_group*rangelen < 2*length(v)
                flag_out, prev_max_group = _grouper_for_int_pool!(prev_groups, groups, prev_max_group, v, _f, minval, rangelen)
            else
                if !seen_non_int
                    resize!(rhashes, nrow(ds))
                    resize!(gslots, sz)
                    seen_non_int = true
                end
                flag_out, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
            end
            flag = flag_out

        elseif CT <: Integer
            v = _columns(ds)[colidx[j]]
            minval = hp_minimum(_f, v)
            if ismissing(minval)
                continue
            end
            maxval = hp_maximum(_f, v)
            (diff, o1) = sub_with_overflow(maxval, minval)
            (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
            (outmult, o3) = mul_with_overflow(Int(rangelen), Int(prev_max_group))
            if !o1 && !o2 && !o3 && maxval < typemax(Int) && (rangelen < div(n,2)) && prev_max_group*rangelen < 2*length(v)
                flag_out, prev_max_group = _grouper_for_int_pool!(prev_groups, groups, prev_max_group, v, _f, minval, rangelen)
            else
                if !seen_non_int
                    resize!(rhashes, nrow(ds))
                    resize!(gslots, sz)
                    seen_non_int = true
                end
                flag_out, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
            end
            flag = flag_out
        else
            if !seen_non_int
                resize!(rhashes, nrow(ds))
                resize!(gslots, sz)
                seen_non_int = true
            end
            v = _columns(ds)[colidx[j]]

            flag_out, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
            flag = flag_out
        end
        !flag && break
    end
    return groups, Int(prev_max_group)
end
