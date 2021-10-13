using .Base: sub_with_overflow, add_with_overflow, mul_with_overflow

function _findstarts_for_indices(x)
    _tmp = zeros(Bool, length(x))
    _tmp[1] = true
    Threads.@threads for i in 2:length(x)
        !isequal(x[i-1], x[i]) ? _tmp[i]=true : nothing
    end
    findall(_tmp)
end

function compute_indices(groups, ngroups, ::Val{T}) where T
    # if ngroups > 50_000_000 && length(groups)/2 < ngroups
    #     _compute_indices_threaded(groups, ngroups, Val(T))
    # else
        # _compute_indices(groups, ngroups, Val(T))
    # end
	idx = Vector{T}(undef, length(groups))
    _fill_idx_for_sort!(idx)
	if length(groups) == ngroups
		return idx, copy(idx)
	end
	starts = _ds_sort_int_missatright_nopermx!(groups, idx, ngroups, 1, Val(T))
	pop!(starts)
	pop!(starts)
	pop!(starts)
	idx, starts
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


# mapreduce for gatherby data

function _fill_mapreduce_col!(x, f, op, init0, y, loc)
    init = init0[1, 1]
    Threads.@threads for i in 1:length(y)
        init0[loc[i], Threads.threadid()] =  op(init0[loc[i],Threads.threadid()], f(y[i]))
    end
    Threads.@threads for i in 1:length(x)
        x[i] = mapreduce(identity, op, view(init0, i, :), init = init)
    end
end

function _fill_mapreduce_col!(x, f::Vector, op, init0, y, loc)
    init = init0[1, 1]
    Threads.@threads for i in 1:length(y)
        init0[loc[i], Threads.threadid()] =  op(init0[loc[i],Threads.threadid()], f[loc[i]](y[i]))
    end
    Threads.@threads for i in 1:length(x)
        x[i] = mapreduce(identity, op, view(init0, i, :), init = init)
    end
end


function gatherby_mapreduce_threaded(gds::GatherBy, f, op, col::ColumnIndex, init::T) where T
    init0 = fill(init, gds.lastvalid, Threads.nthreads())
    init0 = allowmissing(init0)
    res = Tables.allocatecolumn(Union{T, Missing}, gds.lastvalid)
    _fill_mapreduce_col!(res, f, op, init0, _columns(gds.parent)[index(gds.parent)[col]], gds.groups)
    res
end

function _fill_mapreduce_col!(x, f, op, y, loc)
    for i in 1:length(y)
        x[loc[i]] = op(x[loc[i]], f(y[i]))
    end
end

function _fill_mapreduce_col!(x, f::Vector, op, y, loc)
	for i in 1:length(y)
        x[loc[i]] = op(x[loc[i]], f[loc[i]](y[i]))
    end
end


function gatherby_mapreduce(gds::GatherBy, f, op, col::ColumnIndex, init::T; threads = true) where T
	CT = T
    T <: Base.SmallSigned ? CT = Int : nothing
	T <: Base.SmallUnsigned ? CT = UInt : nothing
	T <: Float64 ? CT = Float64 : nothing
	(outmult, o3) = mul_with_overflow(Int(gds.lastvalid), Int(Threads.nthreads()))
	if threads && !o3 && gds.lastvalid*Threads.nthreads() <= 1_000_000 && nrow(gds.parent) > 50_000_000
		return gatherby_mapreduce_threaded(gds, f, op, col, CT(init))
	end
    res = Tables.allocatecolumn(Union{CT, Missing}, gds.lastvalid)
    fill!(res, init)
    _fill_mapreduce_col!(res, f, op, _columns(gds.parent)[index(gds.parent)[col]], gds.groups)
    res
end

_gatherby_maximum(gds, col; f = identity, threads = true) = gatherby_mapreduce(gds, f, _stat_max_fun, col, typemin(nonmissingtype(eltype(gds.parent[!, col]))); threads = threads)
_gatherby_minimum(gds, col; f = identity, threads = true) = gatherby_mapreduce(gds, f, _stat_min_fun, col, typemax(nonmissingtype(eltype(gds.parent[!, col]))); threads = threads)
_gatherby_sum(gds, col; f = identity, threads = true) = gatherby_mapreduce(gds, f, _stat_add_sum, col, zero(Core.Compiler.return_type(f, (eltype(gds.parent[!, col]), ))); threads = threads)
_gatherby_n(gds, col; threads = true) = _gatherby_sum(gds, col, f = _stat_notmissing, threads = threads)
_gatherby_length(gds, col; threads = true) = _gatherby_sum(gds, col, f = x->1, threads = threads)
_gatherby_cntnan(gds, col; threads = true) = _gatherby_sum(gds, col, f = ISNAN, threads = threads)
_gatherby_nmissing(gds, col; threads = true) = _gatherby_sum(gds, col, f = _stat_ismissing, threads = threads)

function _gatherby_mean(gds, col; threads = true)
    sval = _gatherby_sum(gds, col; threads = threads)
    nval = _gatherby_n(gds, col; threads = threads)
    [nval[i] == 0 ? missing : sval[i] / nval[i] for i in 1:length(nval)]
end


# TODO directly calculating var should be a better approach
function _gatherby_var(gds, col; df = true, cal_std = false, threads = true)
    countnan = _gatherby_cntnan(gds, col; threads = threads)
    meanval = _gatherby_mean(gds, col; threads = threads)
    ss = gatherby_mapreduce(gds, [x->abs2(x - meanval[i]) for i in 1:length(meanval)], _stat_add_sum, col, 0.0, threads = threads)
    nval = _gatherby_n(gds, col; threads = threads)
    if cal_std
        [countnan[i] > 0 ? NaN : nval[i] == 0 ? missing : nval[i] == 1 ? 0.0 : sqrt(ss[i] / (nval[i] - Int(df))) for i in 1:length(nval)]
    else
        [countnan[i] > 0 ? NaN : nval[i] == 0 ? missing : nval[i] == 1 ? 0.0 : (ss[i] / (nval[i] - Int(df))) for i in 1:length(nval)]
    end
end
_gatherby_std(gds, col; df = true, threads = true) = _gatherby_var(gds, col; df = df, cal_std = true, threads = threads)


const FAST_GATHERBY_REDUCTION = [sum, length, minimum, maximum, mean, var, std, n, nmissing]


function _fast_gatherby_reduction(gds, ms)
    !(gds isa GatherBy) && return false
    gds.groups == nothing && return false
    for i in 1:length(ms)
        if (ms[i].second.first isa Expr) && ms[i].second.first.head == :BYROW
        elseif (ms[i].second.first isa Base.Callable)
            flag = ms[i].second.first ∈ FAST_GATHERBY_REDUCTION
            !flag && return false
        end
    end
    return true
end


function _fast_gatherby_groups_to_res!(outres, x, grp)
    for i in 1:length(x)
        outres[grp[i]] = x[i]
    end
end


function _fast_gatherby_combine_f_barrier(gds, col, newds, mssecond, mslast, newds_lookup, grp, ngrps)

    if !(mssecond isa Expr)
        if mssecond == sum
            push!(_columns(newds), _gatherby_sum(gds, col))
        elseif mssecond == maximum
            push!(_columns(newds), _gatherby_maximum(gds, col))
        elseif mssecond == minimum
            push!(_columns(newds), _gatherby_minimum(gds, col))
        elseif mssecond == mean
            push!(_columns(newds), _gatherby_mean(gds, col))
        elseif mssecond == mean
            push!(_columns(newds), _gatherby_mean(gds, col))
        elseif mssecond == var
            push!(_columns(newds), _gatherby_var(gds, col, df = true))
        elseif mssecond == std
            push!(_columns(newds), _gatherby_std(gds, col, df = true))
        elseif mssecond == length
            push!(_columns(newds), _gatherby_length(gds, col))
        elseif mssecond == IMD.n
            push!(_columns(newds), _gatherby_n(gds, col))
        else mssecond == IMD.nmissing
            push!(_columns(newds), _gatherby_nmissing(gds, col))
        end


    elseif (mssecond isa Expr) && mssecond.head == :BYROW
        push!(_columns(newds), byrow(newds, mssecond.args[1], col; mssecond.args[2]...))
    else
        throw(ArgumentError("`combine` doesn't support $(msfirst=>mssecond=>mslast) combination"))
    end
end

function _fast_gatherby_combine_f_barrier!(gds, col, res, mssecond, mslast, grp, ngrps, i, threads)

        if mssecond == sum
            res[i] =  _gatherby_sum(gds, col; threads = threads)
        elseif mssecond == maximum
            res[i] =  _gatherby_maximum(gds, col; threads = threads)
        elseif mssecond == minimum
            res[i] =  _gatherby_minimum(gds, col; threads = threads)
        elseif mssecond == mean
            res[i] =  _gatherby_mean(gds, col; threads = threads)
        elseif mssecond == mean
            res[i] =  _gatherby_mean(gds, col; threads = threads)
        elseif mssecond == var
            res[i] =  _gatherby_var(gds, col; threads = threads, df = true)
        elseif mssecond == std
            res[i] =  _gatherby_std(gds, col; threads = threads, df = true)
        elseif mssecond == length
            res[i] =  _gatherby_length(gds, col; threads = threads)
        elseif mssecond == IMD.n
            res[i] =  _gatherby_n(gds, col; threads = threads)
        else mssecond == IMD.nmissing
            res[i] =  _gatherby_nmissing(gds, col; threads = threads)
        end
end


function _combine_fast_gatherby_reduction(gds, ms, newlookup, new_nm)
    groupcols = gds.groupcols
    ngroups = gds.lastvalid
    groups = gds.groups

    all_names = _names(gds.parent)

    newds_idx = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}(), Int[], Bool[], false, [], Int[], 1, false)

    newds = Dataset([], newds_idx)
    newds_lookup = index(newds).lookup
    var_cnt = 1
    for j in 1:length(groupcols)
        addmissing = false
        _tmpres = allocatecol(gds.parent[!, groupcols[j]].val, ngroups, addmissing = addmissing)
        if DataAPI.refpool(_tmpres) !== nothing
            _fast_gatherby_groups_to_res!(_tmpres.refs, _columns(gds.parent)[groupcols[j]].refs, groups)
            push!(_columns(newds), _tmpres)
        else
            _fast_gatherby_groups_to_res!(_tmpres, _columns(gds.parent)[groupcols[j]], groups)
            push!(_columns(newds), _tmpres)
        end
        push!(index(newds), new_nm[var_cnt])
        setformat!(newds, new_nm[var_cnt] => get(index(gds.parent).format, groupcols[j], identity))
        var_cnt += 1
    end

	if all(x->!(x.second.first isa Expr), ms)
		all_res = Vector{Any}(undef, length(ms))
		Threads.@threads for i in 1:length(ms)
			_fast_gatherby_combine_f_barrier!(gds, ms[i].first, all_res, ms[i].second.first, ms[i].second.second, groups, ngroups, i, length(ms) < Threads.nthreads()/10)
		end
		for i in 1:length(ms)
			push!(_columns(newds), all_res[i])
			if !haskey(index(newds), ms[i].second.second)
				push!(index(newds), ms[i].second.second)
			end
		end

	else
		for i in 1:length(ms)
			_fast_gatherby_combine_f_barrier(gds, ms[i].first, newds, ms[i].second.first, ms[i].second.second, newds_lookup, groups, ngroups)
			if !haskey(index(newds), ms[i].second.second)
				push!(index(newds), ms[i].second.second)
			end

		end
	end
    # grouping information for the output dataset
    # append!(index(newds).sortedcols, index(newds)[index(gds.parent).names[groupcols]])
    # append!(index(newds).rev, index(gds.parent).rev)
    # append!(index(newds).perm, collect(1:total_lengths))
    # # index(newds).grouped[] = true
    # index(newds).ngroups[] = ngroups
    # append!(index(newds).starts, collect(1:total_lengths))
    # for i in 2:(length(new_lengths))
    #     index(newds).starts[i] = new_lengths[i - 1]+1
    # end
    newds
end
