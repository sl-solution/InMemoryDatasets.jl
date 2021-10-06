using .Base: sub_with_overflow, add_with_overflow, mul_with_overflow

function _findstarts_for_indices(x)
    _tmp = zeros(Bool, length(x))
    _tmp[1] = true
    Threads.@threads for i in 2:length(x)
        !isequal(x[i-1], x[i]) ? _tmp[i]=true : nothing
    end
    findall(_tmp)
end

function _compute_indices_threaded(groups, ngroups, ::Val{T}; a = HeapSortAlg()) where T
    idx = Vector{T}(undef, length(groups))
    _fill_idx_for_sort!(idx)
    minval = 1
    maxval = ngroups
    n = length(groups)
    rangelen = ngroups
    if rangelen < div(n,2)
        int_where = [Vector{T}(undef, rangelen + 2) for _ in 1:Threads.nthreads()]
        int_permcpy = copy(idx)
        hp_ds_sort_int!(groups, idx, int_permcpy, int_where, rangelen, minval, false, a, Base.Order.Forward)

    else
        hp_ds_sort!(groups, idx, a, Base.Order.Forward)
    end
    idx, _findstarts_for_indices(groups)
end




# TODO copied from DataFrames.jl, can we optimise it?
function _compute_indices(groups, ngroups, ::Val{T}) where T
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


function compute_indices(groups, ngroups, ::Val{T}) where T
    if ngroups > 50_000_000
        _compute_indices_threaded(groups, ngroups, Val(T))
    else
        _compute_indices(groups, ngroups, Val(T))
    end
end

# fast combine for gatherby data

mutable struct GatherBy
    parent::Dataset
    groupcols
    groups
    lastvalid
    mapformats::Bool
    perm
    starts
end


nrow(ds::GatherBy) = nrow(ds.parent)
ncol(ds::GatherBy) = ncol(ds.parent)
Base.names(ds::GatherBy, kwargs...) = names(ds.parent, kwargs...)
_names(ds::GatherBy) = _names(ds.parent)
_columns(ds::GatherBy) = _columns(ds.parent)
index(ds::GatherBy) = index(ds.parent)
Base.parent(ds::GatherBy) = ds.parent


Base.summary(gds::GatherBy) =
        @sprintf("%d×%d View of GatherBy Dataset, Gathered by: %s", size(gds.parent)..., join(_names(gds.parent)[gds.groupcols], " ,"))


function Base.show(io::IO, gds::GatherBy;

                   kwargs...)
     _show(io, view(gds.parent, _get_perms(gds), :); title = summary(gds), kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", gds::GatherBy;
          kwargs...) =
    show(io, gds; title = summary(gds), kwargs...)



function gatherby(ds::Dataset, cols::MultiColumnIndex; mapformats = true)
    colsidx = index(ds)[cols]
    a = _gather_groups(ds, colsidx, nrow(ds)<typemax(Int32) ? Val(Int32) : Val(Int64), mapformats = mapformats)
    GatherBy(ds, colsidx, a[1], a[3], mapformats, nothing, nothing)
end
gatherby(ds::Dataset, col::ColumnIndex; mapformats = true) = gatherby(ds, [col], mapformats = mapformats)

function _fill_mapreduce_col!(x, f, op, init0, y, loc)
    init = init0[1, 1]
    Threads.@threads for i in 1:length(y)
        init0[loc[i], Threads.threadid()] =  op(init0[loc[i],Threads.threadid()], f(y[i]))
    end
    Threads.@threads for i in 1:length(x)
        x[i] = mapreduce(identity, op, init0[i, :], init = init)
    end
end

function gatherby_mapreduce(gds::GatherBy, f, op, col::ColumnIndex, init::T) where T
    init0 = fill(init, gds.lastvalid, Threads.nthreads())
    res = Tables.allocatecolumn(Union{T, Missing}, gds.lastvalid)
    _fill_mapreduce_col!(res, f, op, init0, _columns(gds.parent)[index(gds.parent)[col]], gds.groups)
    res
end
