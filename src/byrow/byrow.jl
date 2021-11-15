struct _DUMMY_STRUCT
end

# anymissing(::_DUMMY_STRUCT) = false
nunique(::_DUMMY_STRUCT) =  false
stdze!(::_DUMMY_STRUCT) = false
stdze(::_DUMMY_STRUCT) = false


byrow(ds::AbstractDataset, ::typeof(sum), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_sum(ds, by, cols) : row_sum(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(sum), col::ColumnIndex; by = identity, threads = true) = byrow(ds, sum, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(prod), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_prod(ds, by, cols) : row_prod(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(prod), col::ColumnIndex; by = identity, threads = true) = byrow(ds, prod, [col]; by = by, threads = threads)


byrow(ds::AbstractDataset, ::typeof(count), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = isequal(true), threads = true) = threads ? hp_row_count(ds, by, cols) : row_count(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(count), col::ColumnIndex; by = isequal(true), threads = true) = byrow(ds, count, [col], by = by, threads = threads)

# byrow(ds::AbstractDataset, ::typeof(anymissing), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_anymissing(ds, cols)

function byrow(ds::AbstractDataset, ::typeof(any), cols::MultiColumnIndex = :; by = x->isequal(true, x), threads = true)
	colsidx = index(ds)[cols]
	if by isa AbstractVector
		bys = map(_bool, by)
		threads ? hp_row_any_multi(ds, bys, colsidx) : row_any_multi(ds, bys, colsidx)
	else
		bys = repeat([_bool(by)], length(colsidx))
		threads ? hp_row_any_multi(ds, bys, colsidx) : row_any_multi(ds, bys, colsidx)
	end
end

byrow(ds::AbstractDataset, ::typeof(any), col::ColumnIndex; by = x->isequal(true, x), threads = true) = byrow(ds, any, [col]; by = by, threads = threads)

function byrow(ds::AbstractDataset, ::typeof(all), cols::MultiColumnIndex = :; by = x->isequal(true, x), threads = true)
	colsidx = index(ds)[cols]
	if by isa AbstractVector
		bys = map(_bool, by)
		threads ? hp_row_all_multi(ds, bys, colsidx) : row_all_multi(ds, bys, colsidx)
	else
		bys = repeat([_bool(by)], length(colsidx))
		threads ? hp_row_all_multi(ds, bys, colsidx) : row_all_multi(ds, bys, colsidx)
	end
end
byrow(ds::AbstractDataset, ::typeof(all), col::ColumnIndex; by = x->isequal(true, x), threads = true) = byrow(ds, all, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(coalesce), cols::MultiColumnIndex; threads = true) = threads ? hp_row_coalesce(ds, cols) : row_coalesce(ds, cols)


byrow(ds::AbstractDataset, ::typeof(mean), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_mean(ds, by, cols) : row_mean(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(mean), col::ColumnIndex; by = identity, threads = true) = byrow(ds, mean, [col]; by = by, threads = threads)


byrow(ds::AbstractDataset, ::typeof(maximum), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_maximum(ds, by, cols) : row_maximum(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(maximum), col::ColumnIndex; by = identity, threads = true) = byrow(ds, maximum, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(minimum), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_minimum(ds, by, cols) : row_minimum(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(minimum), col::ColumnIndex; by = identity, threads = true) = byrow(ds, minimum, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(var), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, dof = true, threads = true) = threads ? hp_row_var(ds, by, cols; dof = dof) : row_var(ds, by, cols; dof = dof)
byrow(ds::AbstractDataset, ::typeof(var), col::ColumnIndex; by = identity, dof = true, threads = true) = byrow(ds, var, [col]; by = by, dof = dof, threads = threads)

byrow(ds::AbstractDataset, ::typeof(std), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, dof = true, threads = true) = threads ? hp_row_std(ds, by, cols; dof = dof) : row_std(ds, by, cols; dof = dof)
byrow(ds::AbstractDataset, ::typeof(std), col::ColumnIndex; by = identity, dof = true, threads = true) = byrow(ds, std, [col]; by = by, dof = dof, threads = threads)

byrow(ds::AbstractDataset, ::typeof(nunique), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); by = identity, count_missing = true) = row_nunique(ds, by, cols; count_missing = count_missing)
byrow(ds::AbstractDataset, ::typeof(nunique), col::ColumnIndex; by = identity, count_missing = true) = byrow(ds, nunique, [col]; by = by, count_missing = count_missing)

byrow(ds::AbstractDataset, ::typeof(cumsum), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_cumsum(ds, cols)
byrow(ds::AbstractDataset, ::typeof(cumsum), col::ColumnIndex) = byrow(ds, cumsum, [col])

byrow(ds::AbstractDataset, ::typeof(cumprod!), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_cumprod!(ds, cols)
byrow(ds::AbstractDataset, ::typeof(cumprod!), col::ColumnIndex) = byrow(ds, cumprod!, [col])

byrow(ds::AbstractDataset, ::typeof(cumprod), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_cumprod(ds, cols)
byrow(ds::AbstractDataset, ::typeof(cumprod), col::ColumnIndex) = byrow(ds, cumprod, [col])

byrow(ds::AbstractDataset, ::typeof(cumsum!), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_cumsum!(ds, cols)
byrow(ds::AbstractDataset, ::typeof(cumsum!), col::ColumnIndex) = byrow(ds, cumsum!, [col])

byrow(ds::AbstractDataset, ::typeof(sort), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); threads = true, kwargs...) = threads ? hp_row_sort(ds, cols; kwargs...) : row_sort(ds, cols; kwargs...)
byrow(ds::AbstractDataset, ::typeof(sort), col::ColumnIndex; threads = true, kwargs...) = byrow(ds, sort, [col]; threads = threads, kwargs...)

byrow(ds::AbstractDataset, ::typeof(sort!), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); threads = true, kwargs...) = threads ? hp_row_sort!(ds, cols; kwargs...) : row_sort!(ds, cols; kwargs...)
byrow(ds::AbstractDataset, ::typeof(sort!), col::ColumnIndex; threads = true, kwargs...) = byrow(ds, sort!, [col]; threads = threads, kwargs...)

byrow(ds::AbstractDataset, ::typeof(stdze), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_stdze(ds, cols)

byrow(ds::AbstractDataset, ::typeof(stdze!), cols::MultiColumnIndex = names(ds, Union{Missing, Number})) = row_stdze!(ds, cols)

byrow(ds::AbstractDataset, ::typeof(hash), cols::MultiColumnIndex = :; by = identity, threads = true) = threads ? row_hash_hp(ds, by, cols) : row_hash(ds, by, cols)
byrow(ds::AbstractDataset, ::typeof(hash), col::ColumnIndex; by = identity, threads = true) = byrow(ds, hash, [col]; by = by, threads = threads)

byrow(ds::AbstractDataset, ::typeof(mapreduce), cols::MultiColumnIndex = names(ds, Union{Missing, Number}); op = .+, f = identity,  init = missings(mapreduce(eltype, promote_type, view(_columns(ds),index(ds)[cols])), nrow(ds)), kwargs...) = mapreduce(f, op, view(_columns(ds), index(ds)[cols]), init = init; kwargs...)


function byrow(ds::AbstractDataset, f::Function, cols::MultiColumnIndex; threads = true)
	colsidx = index(ds)[cols]
	length(colsidx) == 1 && return byrow(ds, f, colsidx[1]; threads = threads)
	threads ?  hp_row_generic(ds, f, cols) : row_generic(ds, f, cols)
end
function byrow(ds::AbstractDataset, f::Function, col::ColumnIndex; threads = true)
	if threads
		T = Core.Compiler.return_type(f, (nonmissingtype(eltype(ds[!, col])), ))
		res = Vector{Union{Missing, T}}(undef, nrow(ds))
		_hp_map_a_function!(res, f, ds[!, col].val)
	else
		T = Core.Compiler.return_type(f, (nonmissingtype(eltype(ds[!, col])), ))
		res = Vector{Union{Missing, T}}(undef, nrow(ds))
		map!(f, res, ds[!, col].val)
	end
	res
end
