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
	idx = Vector{T}(undef, length(groups))
    _fill_idx_for_sort!(idx)
	if length(groups) == ngroups
		return idx, copy(idx)
	end
	# TODO we have the same ifelse in sort, probably we need to clean up these into a new function
	if Threads.nthreads() > 1 && length(groups) > Threads.nthreads() && ngroups > 100_000 && ngroups*Threads.nthreads() < length(groups)
		starts = _ds_sort_int_missatright_nopermx_threaded!(groups, idx, ngroups, 1, Val(T))
	elseif Threads.nthreads() > 1 && length(groups) > Threads.nthreads() && ngroups > 100_000
		starts = _ds_sort_int_missatright_nopermx_threaded_lm!(groups, idx, ngroups, 1, Val(T))
	else
		starts = _ds_sort_int_missatright_nopermx!(groups, idx, ngroups, 1, Val(T))
	end
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
Base.copy(gds::GatherBy) = GatherBy(copy(gds.parent), copy(gds.groupcols), copy(gds.groups), gds.lastvalid, gds.mapformats, gds.perm === nothing ? nothing : copy(gds.perm), gds.starts === nothing ? nothing : copy(gds.starts))


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
	if length(_get_perms(gds)) > 200
		_show(io, view(gds.parent, [first(gds.perm, 100);last(gds.perm, 100)], :); title = summary(gds), show_omitted_cell_summary=false, show_row_number  = false, kwargs...)
	else
		_show(io, view(gds.parent, gds.perm, :); title = summary(gds), show_omitted_cell_summary=false, show_row_number  = false, kwargs...)
	end
end

Base.show(io::IO, mime::MIME"text/plain", gds::GatherBy;
          kwargs...) =
    show(io, gds; title = summary(gds), kwargs...)


function _group_creator!(groups, starts, ngroups)
	if ngroups == 1
		fill!(groups, 1)
		return
	end
  	for j in 1:ngroups
		lo = starts[j]
		j == ngroups ? hi = length(groups) : hi = starts[j + 1] - 1
		fill!(view(groups, lo:hi), j)
	end
end

function gatherby(ds::Dataset, cols::MultiColumnIndex; mapformats = true, stable = true, isgathered = false)
    colsidx = index(ds)[cols]
	T = nrow(ds) < typemax(Int32) ? Int32 : Int64
	_check_consistency(ds)
	if isgathered
		# TODO _find_starts_of_groups() needs improvement
		 colindex, ranges, last_valid_index = _find_starts_of_groups(ds, colsidx, Val(T); mapformats = mapformats)
		 groups = Vector{T}(undef, nrow(ds))
		 _group_creator!(groups, ranges, last_valid_index)
		 return GatherBy(ds, colindex, groups, last_valid_index, mapformats, 1:nrow(ds), ranges)
	else
		a = _gather_groups(ds, colsidx, Val(T), mapformats = mapformats, stable = stable)
    	return GatherBy(ds, colsidx, a[1], a[3], mapformats, nothing, nothing)
	end
end
gatherby(ds::Dataset, col::ColumnIndex; mapformats = true, stable = true, isgathered = false) = gatherby(ds, [col], mapformats = mapformats, stable = stable, isgathered = isgathered)


function _fill_mapreduce_col!(x, f, op, y, loc)
    @inbounds for i in 1:length(y)
        x[loc[i]] = op(x[loc[i]], f(y[i]))
    end
end

function _fill_mapreduce_col!(x, f::Vector, op, y, loc)
	@inbounds for i in 1:length(y)
        x[loc[i]] = op(x[loc[i]], f[loc[i]](y[i]))
    end
end


function _fill_mapreduce_col_threaded!(x, f, op, y, loc, nt)
	@sync for thid in 0:nt-1
		Threads.@spawn for i in 1:length(y)
        	@inbounds if loc[i] % nt == thid
				# we need to do more complicated stuff here?
				x[loc[i]] = op(x[loc[i]], f(y[i]))
			end
		end
    end
end

function _fill_mapreduce_col_threaded!(x, f::Vector, op, y, loc, nt)
	@sync for thid in 0:nt-1
		Threads.@spawn for i in 1:length(y)
        	@inbounds if loc[i] % nt == thid
				x[loc[i]] = op(x[loc[i]], f[loc[i]](y[i]))
			end
		end
    end
end




function gatherby_mapreduce(gds::GatherBy, f, op, col::ColumnIndex, nt, init::T) where T
	CT = T
    T <: Base.SmallSigned ? CT = Int : nothing
	T <: Base.SmallUnsigned ? CT = UInt : nothing
	T <: Float64 ? CT = Float64 : nothing
	res = Tables.allocatecolumn(Union{CT, Missing}, gds.lastvalid)
    fill!(res, init)
	if Threads.nthreads() > 1 && gds.lastvalid > 100_000
		_fill_mapreduce_col_threaded!(res, f, op, _columns(gds.parent)[index(gds.parent)[col]], gds.groups, nt)
	else
    	_fill_mapreduce_col!(res, f, op, _columns(gds.parent)[index(gds.parent)[col]], gds.groups)
	end
    res
end

_gatherby_maximum(gds, col; f = identity, nt = Threads.nthreads()) = gatherby_mapreduce(gds, f, _stat_max_fun, col, nt, typemin(nonmissingtype(eltype(gds.parent[!, col]))))
_gatherby_minimum(gds, col; f = identity, nt = Threads.nthreads()) = gatherby_mapreduce(gds, f, _stat_min_fun, col, nt, typemax(nonmissingtype(eltype(gds.parent[!, col]))))
_gatherby_sum(gds, col; f = identity, nt = Threads.nthreads()) = gatherby_mapreduce(gds, f, _stat_add_sum, col, nt, zero(Core.Compiler.return_type(f, (eltype(gds.parent[!, col]), ))))
_gatherby_n(gds, col; nt = Threads.nthreads()) = _gatherby_sum(gds, col, f = _stat_notmissing, nt = nt)
_gatherby_length(gds, col; nt = Threads.nthreads()) = _gatherby_sum(gds, col, f = x->1, nt = nt)
_gatherby_cntnan(gds, col; nt = Threads.nthreads()) = _gatherby_sum(gds, col, f = ISNAN, nt = nt)
_gatherby_nmissing(gds, col; nt = Threads.nthreads()) = _gatherby_sum(gds, col, f = _stat_ismissing, nt = nt)


function _fill_gatherby_mean_barrier!(res, sval, nval)
	@inbounds for i in 1:length(nval)
		if nval[i] == 0
			res[i] = missing
		else
			res[i] = sval[i]/nval[i]
		end
	end
end


function _gatherby_mean(gds, col; nt = Threads.nthreads())
	nt2 = max(div(nt, 2),1)

	t1 = Threads.@spawn _gatherby_sum(gds, col, nt = nt2)
	t2 = Threads.@spawn _gatherby_n(gds, col, nt = nt2)
	sval = fetch(t1)
	nval = fetch(t2)

	T = Core.Compiler.return_type(/, (nonmissingtype(eltype(sval)), nonmissingtype(eltype(nval))))
	res = Vector{Union{Missing, T}}(undef, length(nval))
	_fill_gatherby_mean_barrier!(res, sval, nval)
	res
end

function _fill_gatherby_var_barrier!(res, countnan, meanval, ss, nval, cal_std, dof)

	@inbounds for i in 1:length(nval)
		if cal_std
			if countnan[i] > 0
				res[i] = NaN
			elseif nval[i] == 0
				res[i] = missing
			elseif nval[i] == 1 && dof
				res[i] = missing
			else
				res[i] = sqrt(ss[i]/(nval[i]-Int(dof)))
			end
		else
			if countnan[i] > 0
				res[i] = NaN
			elseif nval[i] == 0
				res[i] = missing
			elseif nval[i] == 1 && dof
				res[i] = missing
			else
				res[i] = ss[i]/(nval[i]-Int(dof))
			end
		end
	end
end

# TODO directly calculating var should be a better approach
function _gatherby_var(gds, col; dof = true, cal_std = false)
	nt = Threads.nthreads()
	nt2 = max(div(nt,2),1)
	t1 = Threads.@spawn _gatherby_cntnan(gds, col, nt = nt2)
	t2 = Threads.@spawn _gatherby_mean(gds, col, nt = nt2)
	meanval = fetch(t2)
	t3 = Threads.@spawn gatherby_mapreduce(gds, [x->abs2(x - meanval[i]) for i in 1:length(meanval)], _stat_add_sum, col, nt2, 0.0)
	t4 = Threads.@spawn _gatherby_n(gds, col, nt = nt2)
	countnan = fetch(t1)
	ss = fetch(t3)
	nval = fetch(t4)
	T = Core.Compiler.return_type(/, (nonmissingtype(eltype(meanval)), nonmissingtype(eltype(nval))))
	res = Vector{Union{Missing, T}}(undef, length(nval))
	_fill_gatherby_var_barrier!(res, countnan, meanval, ss, nval, cal_std, dof)
	res
end
_gatherby_std(gds, col; dof = true) = _gatherby_var(gds, col; dof = dof, cal_std = true)


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
	# the first element of each group is used for the output data set
    @inbounds for i in length(x):-1:1
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
            push!(_columns(newds), _gatherby_var(gds, col, dof = true))
        elseif mssecond == std
            push!(_columns(newds), _gatherby_std(gds, col, dof = true))
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

function _combine_fast_gatherby_reduction(gds, ms, newlookup, new_nm; dropgroupcols = false)
    groupcols = gds.groupcols
    ngroups = gds.lastvalid
    groups = gds.groups

    all_names = _names(gds.parent)

    newds_idx = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}(), Int[], Bool[], false, [], Int[], 1, false)

    newds = Dataset([], newds_idx)
    newds_lookup = index(newds).lookup
    var_cnt = 1
	if !dropgroupcols
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
	end
	for i in 1:length(ms)
		_fast_gatherby_combine_f_barrier(gds, ms[i].first, newds, ms[i].second.first, ms[i].second.second, newds_lookup, groups, ngroups)
		if !haskey(index(newds), ms[i].second.second)
			push!(index(newds), ms[i].second.second)
		end

	end
	newds
end
