using .Base: sub_with_overflow, add_with_overflow, mul_with_overflow
# NOT READY YET
function _create_dictionary_int!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group, minval, ::Val{T}) where T
    offset = 1 - minval
    n = length(v)
    # sz = 2 ^ ceil(Int, log2(n)+1)
    sz = length(gslots)
    # fill!(gslots, 0)
    Threads.@threads for i in 1:sz
        @inbounds gslots[i] = 0
    end
    szm1 = sz - 1
    ngroups = 0
    flag = true
    @inbounds for i in eachindex(rhashes)
        slotix = f(v[i]) + offset
        if ismissing(slotix)
            slotix = sz
        end
        gix = -1
        probe = 0
        while true
            g_row = gslots[slotix]
            if g_row == 0
                gslots[slotix] = i
                gix = ngroups += 1
                break
            #check hash collision
            else
                gix = groups[g_row]
                break
            end
        end
        groups[i] = gix
    end
    if ngroups == n
        flag = false
        return flag, ngroups
    end

    remap = zeros(T, prev_max_group, ngroups)
    ngroups_new = 0
    for i in 1:length(groups)
        if remap[prev_groups[i], groups[i]] == 0
            ngroups_new += 1
            remap[prev_groups[i], groups[i]] = ngroups_new
            prev_groups[i] = remap[prev_groups[i], groups[i]]
        else
            prev_groups[i] = remap[prev_groups[i], groups[i]]
        end
    end
    return flag, ngroups_new
end


function _gather_groups_v2(ds, cols, ::Val{T}; mapformats = false) where T
    colidx = index(ds)[cols]
    _max_level = nrow(ds)
    prev_max_group = UInt(1)
    prev_groups = ones(UInt, nrow(ds))
    groups = Vector{T}(undef, nrow(ds))
    rhashes = Vector{UInt}(undef, nrow(ds))
    sz = max(1 + ((5 * _max_level) >> 2), 16)
    sz = 1 << (8 * sizeof(sz) - leading_zeros(sz - 1))
    @assert 4 * sz >= 5 * _max_level
    gslots = Vector{T}(undef, sz)

    for j in 1:length(colidx)
        _f = identity
        if mapformats
            _f = getformat(ds, colidx[j])
        end

        if DataAPI.refpool(_columns(ds)[colidx[j]]) !== nothing
            if _f == identity
                v = DataAPI.refarray(_columns(ds)[colidx[j]])
            else
                v = DataAPI.refarray(map(_f, _columns(ds)[colidx[j]]))
            end
            _f = identity
        else
            v = _columns(ds)[colidx[j]]
        end
        if Core.Compiler.return_type(_f, (eltype(v),)) <: Union{Missing, Integer}
            _minval = hp_minimum(_f, v)
            if ismissing(_minval)
                continue
            else
                minval::Integer = _minval
            end
            maxval::Integer = hp_maximum(_f, v)
            (diff, o1) = sub_with_overflow(maxval, minval)
            (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
            (outmult, o3) = mul_with_overflow(Int(rangelen), Int(prev_max_group))
            if !o1 && !o2 && !o3 && maxval < typemax(Int) && (rangelen < div(length(v),2)) && prev_max_group*rangelen < 2*length(v)
                flag, prev_max_group = _create_dictionary_int!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group, minval, Val(T))
            else
                flag, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
            end
        else
            flag, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)
        end
        !flag && break
    end
    return Int.(prev_groups), gslots, prev_max_group
end


function gatherby(ds, cols::MultiColumnIndex; mapformats = true)
    final_res, gslots, prev_max_group = _gather_groups_v2(ds, cols, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64); mapformats = mapformats)
    perms, starts = compute_indices(final_res, prev_max_group, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
    colsidx = index(ds)[cols]
    GroupBy(ds, colsidx, perms, starts, prev_max_group)
end
gatherby(ds, col::ColumnIndex; mapformats = true) = gatherby(ds, [col]; mapformats = mapformats)


# TODO copied from DataFrames.jl, can we optimise it?
function compute_indices(groups, ngroups, ::Val{T}) where T
    # count elements in each group
    stops = zeros(T, ngroups+1)
    @inbounds for gix in groups
        stops[gix+1] += 1
    end

    # group start positions in a sorted table
    starts = Vector{T}(undef, ngroups+1)
    if length(starts) > 0
        starts[1] = 1
        @inbounds for i in 1:ngroups
            starts[i+1] = starts[i] + stops[i]
        end
    end

    # define row permutation that sorts them into groups
    rperm = Vector{T}(undef, length(groups))
    copyto!(stops, starts)
    @inbounds for (i, gix) in enumerate(groups)
        rperm[stops[gix+1]] = i
        stops[gix+1] += 1
    end

    # When skipmissing=true was used, group 0 corresponds to missings to drop
    # Otherwise it's empty
    popfirst!(starts)

    return rperm, starts
end
