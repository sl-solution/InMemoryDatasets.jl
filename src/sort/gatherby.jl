using .Base: sub_with_overflow, add_with_overflow, mul_with_overflow

# FIXME it needs more work, e.g it needs to handle missing values
function _gatherby(ds, cols, ::Val{T}; mapformats = true) where T
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

struct GatherBy
    parent::Dataset
    groupcols::Vector{Int}
    groups::Vector{<:Integer}
    ngroups::Int
    mapformats::Bool
end

function gatherby(ds::Dataset, cols::MultiColumnIndex; mapformats = true)
    colsidx = index(ds)[cols]
    a = _gatherby(ds, colsidx, nrow(ds)<typemax(Int32) ? Val(Int32) : Val(Int64), mapformats = mapformats)
    GatherBy(ds, colsidx, a[1], a[2], mapformats)
end
gatherby(ds::Dataset, col::ColumnIndex; mapformats = true) = gatherby(ds, [col], mapformats = mapformats)

function compute_indices(groups::AbstractVector{<:Integer}, ngroups::Integer)
    # count elements in each group
    stops = zeros(Int, ngroups+1)
    @inbounds for gix in groups
        stops[gix+1] += 1
    end

    # group start positions in a sorted table
    starts = Vector{Int}(undef, ngroups+1)
    if length(starts) > 0
        starts[1] = 1
        @inbounds for i in 1:ngroups
            starts[i+1] = starts[i] + stops[i]
        end
    end

    # define row permutation that sorts them into groups
    rperm = Vector{Int}(undef, length(groups))
    copyto!(stops, starts)
    @inbounds for (i, gix) in enumerate(groups)
        rperm[stops[gix+1]] = i
        stops[gix+1] += 1
    end
    stops .-= 1

    # When skipmissing=true was used, group 0 corresponds to missings to drop
    # Otherwise it's empty
    popfirst!(starts)
    popfirst!(stops)

    return rperm, starts, stops
end

Base.summary(gds::GatherBy) =
        @sprintf("%d×%d Gathered Dataset, Gathered by: %s", size(gds.parent)..., join(_names(gds.parent)[gds.groupcols], " ,"))


function Base.show(io::IO, gds::GatherBy;

                   kwargs...)
    _show(io, view(gds.parent,compute_indices(gds.groups, gds.ngroups)[1], :); title = summary(gds), kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", gds::GatherBy;
          kwargs...) =
    show(io, gds; title = summary(gds), kwargs...)


function _fill_gathered_col!(x, y, loc)
    for i in 1:length(y)
        x[loc[i]] = y[i]
    end
end

function _fill_mapreduce_col!(x, f, op, init0, y, loc)
    for i in 1:length(y)
        init0[loc[i]] =  op(init0[loc[i]], f(y[i]))
        x[loc[i]] = init0[loc[i]]
    end
end

function Base.mapreduce(gds::GatherBy, f, op, col::ColumnIndex, init::T) where T
    init0 = fill(init, gds.ngroups)
    res = AbstractVector[]
    for j in gds.groupcols
        push!(res, Tables.allocatecolumn(eltype(_columns(gds.parent)[j]), gds.ngroups))
    end
    push!(res, Tables.allocatecolumn(Union{T, Missing}, gds.ngroups))
    for j in 1:(length(res)-1)
        _fill_gathered_col!(res[j], _columns(gds.parent)[gds.groupcols[j]], gds.groups)
    end
    _fill_mapreduce_col!(res[end], f, op, init0, _columns(gds.parent)[index(gds.parent)[col]], gds.groups)
    newnm = _names(gds.parent)[gds.groupcols]
    push!(newnm, Symbol(_names(gds.parent)[index(gds.parent)[col]], "_agg"))
    newds = Dataset(res, newnm, copycols = false)
    for j in 1:(length(res)-1)
        setformat!(newds, j, getformat(gds.parent, gds.groupcols[j]))
    end
    newds
end
