struct _DUMMY_STRUCT
end

# anymissing(::_DUMMY_STRUCT) = false
nunique(::_DUMMY_STRUCT) =  false
stdze!(::_DUMMY_STRUCT) = false
stdze(::_DUMMY_STRUCT) = false
select(::_DUMMY_STRUCT) = false

byrow(ds::AbstractDataset, ::typeof(sum), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = nrow(ds)>1000) = threads ? hp_row_sum(ds, by, cols) : row_sum(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(sum), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, sum, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(prod), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = nrow(ds)>1000) = threads ? hp_row_prod(ds, by, cols) : row_prod(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(prod), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, prod, [col]; by = by, threads = threads)


byrow(ds::AbstractDataset, ::typeof(count), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = isequal(true), threads = nrow(ds)>1000) = threads ? hp_row_count(ds, by, cols) : row_count(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(count), col::ColumnIndex; by = isequal(true), threads = nrow(ds)>1000) = byrow(ds, count, [col], by = by, threads = threads)

# byrow(ds::AbstractDataset, ::typeof(anymissing), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_anymissing(ds, cols)

function expand_Base_Fix(f, f2)
	if f isa Base.Fix2
		return _bool(x->f.f(f2(x), f.x))
	elseif f isa Base.Fix1
		return _bool(x->f.f(f.x, f2(x)))
	else
		return x->f(f2(x))
	end
end

function byrow(ds::AbstractDataset, ::typeof(any), cols::MultiColumnIndex = :; by = isequal(true), threads = nrow(ds)>1000, mapformats = false)
	colsidx = index(ds)[cols]
	if by isa AbstractVector
		if mapformats
			bys = map((x,y)->expand_Base_Fix(x, getformat(ds, y)), by, colsidx)
		else
			bys = map(_bool, by)
		end
		threads ? hp_row_any_multi(ds, bys, colsidx) : row_any_multi(ds, bys, colsidx)
	else
		if mapformats
			bys = map(y->expand_Base_Fix(by, getformat(ds, y)), colsidx)
		else
			bys = repeat([_bool(by)], length(colsidx))
		end
		threads ? hp_row_any_multi(ds, bys, colsidx) : row_any_multi(ds, bys, colsidx)
	end
end

byrow(ds::AbstractDataset, ::typeof(any), col::ColumnIndex; by = isequal(true), threads = nrow(ds)>1000, mapformats = false) = byrow(ds, any, [col]; by = by, threads = threads, mapformats = mapformats)

function byrow(ds::AbstractDataset, ::typeof(all), cols::MultiColumnIndex = :; by = isequal(true), threads = nrow(ds)>1000, mapformats = false)
	colsidx = index(ds)[cols]
	if by isa AbstractVector
		if mapformats
			bys = map((x,y)->expand_Base_Fix(x, getformat(ds, y)), by, colsidx)
		else
			bys = map(_bool, by)
		end
		threads ? hp_row_all_multi(ds, bys, colsidx) : row_all_multi(ds, bys, colsidx)
	else
		if mapformats
			bys = map(y->expand_Base_Fix(by, getformat(ds, y)), colsidx)
		else
			bys = repeat([_bool(by)], length(colsidx))
		end
		threads ? hp_row_all_multi(ds, bys, colsidx) : row_all_multi(ds, bys, colsidx)
	end
end
byrow(ds::AbstractDataset, ::typeof(all), col::ColumnIndex; by = isequal(true), threads = nrow(ds)>1000, mapformats = false) = byrow(ds, all, [col]; by = by, threads = threads, mapformats = mapformats)

byrow(ds::AbstractDataset, ::typeof(isequal), cols::MultiColumnIndex; with = nothing, threads = nrow(ds)>1000) = row_isequal(ds, cols, by = with, threads = threads)
byrow(ds::AbstractDataset, ::typeof(isequal), cols::ColumnIndex; with = nothing, threads = nrow(ds)>1000) = row_isequal(ds, cols, by = with, threads = threads)


byrow(ds::AbstractDataset, ::typeof(isless), cols::MultiColumnIndex; with, threads = nrow(ds)>1000, rev::Bool = false) = row_isless(ds, cols, with, threads = threads, rev = rev)
byrow(ds::AbstractDataset, ::typeof(isless), col::ColumnIndex; with, threads = nrow(ds)>1000, rev::Bool = false) = row_isless(ds, [col], with, threads = threads, rev = rev)

byrow(ds::AbstractDataset, ::typeof(findfirst), cols::MultiColumnIndex; by = identity, threads = nrow(ds)> 1000) = row_findfirst(ds, by, cols; threads = threads)
byrow(ds::AbstractDataset, ::typeof(findlast), cols::MultiColumnIndex; by = identity, threads = nrow(ds)> 1000) = row_findlast(ds, by, cols; threads = threads)

byrow(ds::AbstractDataset, ::typeof(select), cols::MultiColumnIndex; with, threads = nrow(ds)>1000) = row_select(ds, cols, with, threads = threads)

byrow(ds::AbstractDataset, ::typeof(fill!), cols::MultiColumnIndex; with , by = ismissing, threads = nrow(ds)>1000, rolling = false) = row_fill!(ds, cols, with, f = by, threads = threads, rolling = rolling)
byrow(ds::AbstractDataset, ::typeof(fill!), col::ColumnIndex; with , by = ismissing, threads = nrow(ds)>1000, rolling = false) = byrow(ds, fill!, [col], with = with, by = by, threads = threads, rolling = rolling)
byrow(ds::AbstractDataset, ::typeof(fill), cols::MultiColumnIndex; with , by = ismissing, threads = nrow(ds)>1000, rolling = false) = row_fill!(copy(ds), cols, with, f = by, threads = threads, rolling = rolling)
byrow(ds::AbstractDataset, ::typeof(fill), col::ColumnIndex; with , by = ismissing, threads = nrow(ds)>1000, rolling = false) = byrow(copy(ds), fill!, [col], with = with, by = by, threads = threads, rolling = rolling)


byrow(ds::AbstractDataset, ::typeof(coalesce), cols::MultiColumnIndex; threads = nrow(ds)>1000) = threads ? hp_row_coalesce(ds, cols) : row_coalesce(ds, cols)

byrow(ds::AbstractDataset, ::typeof(mean), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = nrow(ds)>1000) = threads ? hp_row_mean(ds, by, cols) : row_mean(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(mean), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, mean, [col]; by = by, threads = threads)


byrow(ds::AbstractDataset, ::typeof(maximum), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = nrow(ds)>1000) = threads ? hp_row_maximum(ds, by, cols) : row_maximum(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(maximum), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, maximum, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(minimum), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = nrow(ds)>1000) = threads ? hp_row_minimum(ds, by, cols) : row_minimum(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(minimum), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, minimum, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(argmin), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = nrow(ds)>1000) = row_argmin(ds, by, cols, threads = threads)
byrow(ds::AbstractDataset, ::typeof(argmin), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, argmin, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(argmax), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = nrow(ds)>1000) = row_argmax(ds, by, cols, threads = threads)
byrow(ds::AbstractDataset, ::typeof(argmax), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, argmax, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(var), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, dof = true, threads = nrow(ds)>1000) = threads ? hp_row_var(ds, by, cols; dof = dof) : row_var(ds, by, cols; dof = dof)
byrow(ds::AbstractDataset, ::typeof(var), col::ColumnIndex; by = identity, dof = true, threads = nrow(ds)>1000) = byrow(ds, var, [col]; by = by, dof = dof, threads = threads)

byrow(ds::AbstractDataset, ::typeof(std), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, dof = true, threads = nrow(ds)>1000) = threads ? hp_row_std(ds, by, cols; dof = dof) : row_std(ds, by, cols; dof = dof)
byrow(ds::AbstractDataset, ::typeof(std), col::ColumnIndex; by = identity, dof = true, threads = nrow(ds)>1000) = byrow(ds, std, [col]; by = by, dof = dof, threads = threads)

byrow(ds::AbstractDataset, ::typeof(nunique), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, count_missing = true) = row_nunique(ds, by, cols; count_missing = count_missing)
byrow(ds::AbstractDataset, ::typeof(nunique), col::ColumnIndex; by = identity, count_missing = true) = byrow(ds, nunique, [col]; by = by, count_missing = count_missing)

byrow(ds::AbstractDataset, ::typeof(cumsum), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cumsum(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cumsum), col::ColumnIndex; missings = :ignore) = byrow(ds, cumsum, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(cumprod!), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cumprod!(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cumprod!), col::ColumnIndex; missings = :ignore) = byrow(ds, cumprod!, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(cumprod), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cumprod(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cumprod), col::ColumnIndex; missings = :ignore) = byrow(ds, cumprod, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(cumsum!), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cumsum!(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cumsum!), col::ColumnIndex; missings = :ignore) = byrow(ds, cumsum!, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(cummin!), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cummin!(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cummin!), col::ColumnIndex; missings = :ignore) = byrow(ds, cummin!, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(cummin), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cummin(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cummin), col::ColumnIndex; missings = :ignore) = byrow(ds, cummin, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(cummax!), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cummax!(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cummax!), col::ColumnIndex; missings = :ignore) = byrow(ds, cummax!, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(cummax), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); missings = :ignore) = row_cummax(ds, cols, missings = missings)
byrow(ds::AbstractDataset, ::typeof(cummax), col::ColumnIndex; missings = :ignore) = byrow(ds, cummax, [col], missings = missings)

byrow(ds::AbstractDataset, ::typeof(sort), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); threads = true, kwargs...) = threads ? hp_row_sort(ds, cols; kwargs...) : row_sort(ds, cols; kwargs...)
byrow(ds::AbstractDataset, ::typeof(sort), col::ColumnIndex; threads = true, kwargs...) = byrow(ds, sort, [col]; threads = threads, kwargs...)

byrow(ds::AbstractDataset, ::typeof(sort!), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); threads = true, kwargs...) = threads ? hp_row_sort!(ds, cols; kwargs...) : row_sort!(ds, cols; kwargs...)
byrow(ds::AbstractDataset, ::typeof(sort!), col::ColumnIndex; threads = true, kwargs...) = byrow(ds, sort!, [col]; threads = threads, kwargs...)

byrow(ds::AbstractDataset, ::typeof(issorted), cols::MultiColumnIndex; threads = nrow(ds)>1000, rev = false) = threads ? hp_row_issorted(ds, cols; rev = rev) : row_issorted(ds, cols; rev = rev)

byrow(ds::AbstractDataset, ::typeof(stdze), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_stdze(ds, cols)

byrow(ds::AbstractDataset, ::typeof(stdze!), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_stdze!(ds, cols)

byrow(ds::AbstractDataset, ::typeof(hash), cols::MultiColumnIndex = :; by = identity, threads = nrow(ds)>1000) = threads ? row_hash_hp(ds, by, cols) : row_hash(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(hash), col::ColumnIndex; by = identity, threads = nrow(ds)>1000) = byrow(ds, hash, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(mapreduce), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); op = .+, f = identity,  init = missings(mapreduce(eltype, promote_type, view(_columns(ds),index(ds)[cols])), nrow(ds)), kwargs...) = mapreduce(f, op, eachcol(ds[!, cols]), init = init; kwargs...)


function byrow(ds::AbstractDataset, f::Function, cols::MultiColumnIndex; threads = nrow(ds)>1000)
	colsidx = index(ds)[cols]
	length(colsidx) == 1 && return byrow(ds, f, colsidx[1]; threads = threads)
	threads ?  hp_row_generic(ds, f, cols) : row_generic(ds, f, cols)
end
function byrow(ds::AbstractDataset, f::Function, col::ColumnIndex; threads = nrow(ds)>1000)
	if threads
		T = Core.Compiler.return_type(f, (nonmissingtype(eltype(ds[!, col])), ))
		res = Vector{Union{Missing, T}}(undef, nrow(ds))
		_hp_map_a_function!(res, f, _columns(ds)[index(ds)[col]])
	else
		T = Core.Compiler.return_type(f, (nonmissingtype(eltype(ds[!, col])), ))
		res = Vector{Union{Missing, T}}(undef, nrow(ds))
		map!(f, res, _columns(ds)[index(ds)[col]])
	end
	res
end
