struct _DUMMY_STRUCT
end

# anymissing(::_DUMMY_STRUCT) = false
nunique(::_DUMMY_STRUCT) =  false
stdze!(::_DUMMY_STRUCT) = false
stdze(::_DUMMY_STRUCT) = false


byrow(ds::AbstractDataset, ::typeof(sum), cols = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_sum(ds, by, cols) : row_sum(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(prod), cols = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_prod(ds, by, cols) : row_prod(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(count), cols = names(ds, Union{Missing, Number}); by = x->true, threads = true) = threads ? hp_row_count(ds, by, cols) : row_count(ds, by, cols)

# byrow(ds::AbstractDataset, ::typeof(anymissing), cols = names(ds, Union{Missing, Number})) = row_anymissing(ds, cols)

byrow(ds::AbstractDataset, ::typeof(any), cols = :; by = x->isequal(true, x), threads = true) = threads ? hp_row_any(ds, by, cols) : row_any(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(all), cols = :; by = x->isequal(true, x), threads = true) = threads ? hp_row_all(ds, by, cols) : row_all(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(mean), cols = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_mean(ds, by, cols) : row_mean(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(maximum), cols = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_maximum(ds, by, cols) : row_maximum(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(minimum), cols = names(ds, Union{Missing, Number}); by = identity, threads = true) = threads ? hp_row_minimum(ds, by, cols) : row_minimum(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(var), cols = names(ds, Union{Missing, Number}); by = identity, dof = true, threads = true) = threads ? hp_row_var(ds, by, cols; dof = dof) : row_var(ds, by, cols; dof = dof)

byrow(ds::AbstractDataset, ::typeof(std), cols = names(ds, Union{Missing, Number}); by = identity, dof = true, threads = true) = threads ? hp_row_std(ds, by, cols; dof = dof) : row_std(ds, by, cols; dof = dof)

byrow(ds::AbstractDataset, ::typeof(nunique), cols = names(ds, Union{Missing, Number}); by = identity, count_missing = true) = row_nunique(ds, by, cols; count_missing = count_missing)

byrow(ds::AbstractDataset, ::typeof(cumsum), cols = names(ds, Union{Missing, Number})) = row_cumsum(ds, cols)

byrow(ds::AbstractDataset, ::typeof(cumprod!), cols = names(ds, Union{Missing, Number})) = row_cumprod!(ds, cols)

byrow(ds::AbstractDataset, ::typeof(cumprod), cols = names(ds, Union{Missing, Number})) = row_cumprod(ds, cols)

byrow(ds::AbstractDataset, ::typeof(cumsum!), cols = names(ds, Union{Missing, Number})) = row_cumsum!(ds, cols)

byrow(ds::AbstractDataset, ::typeof(sort), cols = names(ds, Union{Missing, Number}); threads = true, kwargs...) = threads ? hp_row_sort(ds, cols; kwargs...) : row_sort(ds, cols; kwargs...)

byrow(ds::AbstractDataset, ::typeof(sort!), cols = names(ds, Union{Missing, Number}); threads = true, kwargs...) = threads ? hp_row_sort!(ds, cols; kwargs...) : row_sort!(ds, cols; kwargs...)

byrow(ds::AbstractDataset, ::typeof(stdze), cols = names(ds, Union{Missing, Number})) = row_stdze(ds, cols)

byrow(ds::AbstractDataset, ::typeof(stdze!), cols = names(ds, Union{Missing, Number})) = row_stdze!(ds, cols)

byrow(ds::AbstractDataset, ::typeof(hash), cols = :; by = identity, threads = true) = threads ? row_hash_hp(ds, by, cols) : row_hash(ds, by, cols)

byrow(ds::AbstractDataset, ::typeof(mapreduce), cols = names(ds, Union{Missing, Number}); op = .+, kwargs...) = mapreduce(identity, op, eachcol(ds[!, cols]); kwargs...)

byrow(ds::AbstractDataset, ::typeof(reduce), cols = names(ds, Union{Missing, Number}); op = .+, kwargs...) = reduce(op, eachcol(ds[!, cols]); kwargs...)

byrow(ds::AbstractDataset, f::Function, cols) = row_generic(ds, f, cols)
#
# function byrow(f::Function, ds::AbstractDataset, col::DataFrames.ColumnIndex; kwargs...)
#     _temp_fun(x) = f(x; kwargs...)
#     _temp_fun.(ds[!, col])
# end
# byrow(f::Function, ds::AbstractDataset, cols; kwargs...) = f.(eachrow(ds[!, cols]); kwargs...)
