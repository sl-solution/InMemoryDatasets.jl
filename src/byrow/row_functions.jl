_add_sum(x, y) = Base.add_sum(x, y)
_add_sum(x, ::Missing) = x
_add_sum(::Missing, x) = x
_add_sum(::Missing, ::Missing) = missing
_mul_prod(x, y) = Base.mul_prod(x, y)
_mul_prod(x, ::Missing) = x
_mul_prod(::Missing, x) = x
_mul_prod(::Missing, ::Missing) = missing
_min_fun(x, y) = min(x, y)
_min_fun(x, ::Missing) = x
_min_fun(::Missing, y) = y
_min_fun(::Missing, ::Missing) = missing
_max_fun(x, y) = max(x, y)
_max_fun(x, ::Missing) = x
_max_fun(::Missing, y) = y
_max_fun(::Missing, ::Missing) = missing
_bool(f) = x->f(x)::Bool

struct _Prehashed
    hash::UInt64
end
Base.hash(x::_Prehashed) = x.hash


function row_sum(ds::AbstractDataset, f::Function,  cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _op_for_sum!(x, y) = x .= _add_sum.(x, f.(y))
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    mapreduce(identity, _op_for_sum!, view(_columns(ds),colsidx), init = init0)
end
row_sum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_sum(ds, identity, cols)


function row_prod(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _op_for_prod!(x, y) = x .= _mul_prod.(x, f.(y))
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : one(T))
    mapreduce(identity, _op_for_prod!, view(_columns(ds),colsidx), init = init0)
end
row_prod(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_prod(ds, identity, cols)


function row_count(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    _op_for_count!(x, y) = x .+= (_bool(f).(y))
    mapreduce(identity, _op_for_count!, view(_columns(ds),colsidx), init = zeros(Int32, size(ds,1)))
end
row_count(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_count(ds, x->true, cols)

function row_any(ds::AbstractDataset, f::Function, cols = :)
    colsidx = index(ds)[cols]
    _op_bool_add(x::Bool,y::Bool) = x | y ? true : false
    op_for_any!(x,y) = x .= _op_bool_add.(x, _bool(f).(y))
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, op_for_any!, view(_columns(ds),colsidx), init = zeros(Bool, size(ds,1)))
end
row_any(ds::AbstractDataset, cols = :) = row_any(ds, isequal(true), cols)

function row_all(ds::AbstractDataset, f::Function, cols = :)
    colsidx = index(ds)[cols]
    _op_bool_mult(x::Bool,y::Bool) = x & y ? true : false
    op_for_all!(x,y) = x .= _op_bool_mult.(x, _bool(f).(y))
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, op_for_all!, view(_columns(ds),colsidx), init = ones(Bool, size(ds,1)))
end
row_all(ds::AbstractDataset, cols = :) = row_all(ds, isequal(true), cols)

# this is a general rule for order of arguments in isequal, isless, findfirst, ...
# if the keyword argument is `with` then eq(y, with)
# if the keyword argument is `item` then eq(item, y)

function _op_for_isequal!(x,y, x1)
    x .&= isequal.(y, x1)
    x
end
function hp_op_for_isequal!(x,y, x1)
    Threads.@threads for i in 1:length(x)
        x[i] &= isequal(y[i], x1[i])
    end
    x
end

function row_isequal(ds::AbstractDataset, cols = :; by::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex, Nothing} = nothing, threads = true)
    colsidx = index(ds)[cols]
    if !(by isa ColumnIndex) && by !== nothing
        @assert length(by) == nrow(ds) "to compare values of selected columns in each row, the length of the passed vector and the number of rows must match"
    end
    if by isa SubDatasetColumn || by isa DatasetColumn
        x1 = __!(by)
    elseif by isa ColumnIndex
        x1 = _columns(ds)[index(ds)[by]]
    elseif by === nothing
        x1 = _columns(ds)[colsidx[1]]
    else
        x1 = by
    end
    init0 = ones(Bool, nrow(ds))    
    if threads
        mapreduce(identity, (x,y)->hp_op_for_isequal!(x,y,x1), view(_columns(ds),colsidx), init = init0)
    else
        mapreduce(identity, (x,y)->_op_for_isequal!(x,y,x1), view(_columns(ds),colsidx), init = init0)
    end
end

function _op_for_isless!(x, y, vals, rev,lt)
    if !rev
        x .&= lt.(y, vals)
    else
        x .&= lt.(vals, y)
    end
    x
end
function hp_op_for_isless!(x, y, vals, rev,lt)
    if !rev
        Threads.@threads for i in 1:length(x)
            x[i] &= lt(y[i], vals[i])
        end
    else
        Threads.@threads for i in 1:length(x)
            x[i] &= lt(vals[i], y[i])
        end
    end
    x
end

function row_isless(ds::AbstractDataset, cols, colselector::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex}; threads = true, rev = false, lt = isless)
    if !(colselector isa ColumnIndex)
        @assert length(colselector) == nrow(ds) "to compare values of selected columns in each row, the length of the passed vector and the number of rows must match"
    end
    colsidx = index(ds)[cols]
    if colselector isa SubDatasetColumn || colselector isa DatasetColumn
        colselector = __!(colselector)
    end
    if colselector isa ColumnIndex
        colselector = _columns(ds)[index(ds)[colselector]]
    end
    init0 = ones(Bool, nrow(ds))
    if threads
        mapreduce(identity, (x,y)->hp_op_for_isless!(x,y,colselector, rev, lt), view(_columns(ds),colsidx), init = init0)
    else
        mapreduce(identity, (x,y)->_op_for_isless!(x,y,colselector,rev, lt), view(_columns(ds),colsidx), init = init0)
    end
end


# TODO probably we should use this approach instead of mapreduce_indexed
function _op_for_findfirst!(x, y, f, idx, missref)
    idx[] += 1
    x .= ifelse.(isequal.(missref, x) .& isequal.(true, f.(y)), idx, x)
    x
end

function hp_op_for_findfirst!(x, y, f, idx, missref)
    idx[] += 1
    Threads.@threads for i in 1:length(x)
        x[i] = ifelse(isequal(missref, x[i]) & isequal(true, f(y[i])), idx[], x[i])
    end
    x
end
function _op_for_findfirst!(x, y, item, idx, missref, eq)
    idx[] += 1
    x .= ifelse.(isequal.(missref, x) .& eq.(item, y), idx, x)
    x
end

function hp_op_for_findfirst!(x, y, item, idx, missref, eq)
    idx[] += 1
    Threads.@threads for i in 1:length(x)
        x[i] = ifelse(isequal(missref, x[i]) & eq(item[i], y[i]), idx[], x[i])
    end
    x
end

function _op_for_findlast!(x, y, f, idx, missref)
    idx[] += 1
    x .= ifelse.(isequal.(true, f.(y)), idx, x)
    x
end

function hp_op_for_findlast!(x, y, f, idx, missref)
    idx[] += 1
    Threads.@threads for i in 1:length(x)
        x[i] = ifelse(isequal(true, f(y[i])), idx[], x[i])
    end
    x
end

function _op_for_findlast!(x, y, item, idx, missref, eq)
    idx[] += 1
    x .= ifelse.(eq.(item, y), idx, x)
    x
end

function hp_op_for_findlast!(x, y, item, idx, missref, eq)
    idx[] += 1
    Threads.@threads for i in 1:length(x)
        x[i] = ifelse(eq(item[i], y[i]), idx[], x[i])
    end
    x
end

# TODO probably we should use threads argument instead of seperate functions for hp version
function row_findfirst(ds::AbstractDataset, f, cols = names(ds, Union{Missing, Number});item::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex, Nothing} = nothing, threads = true, eq = isequal)
    if !(item isa ColumnIndex) && item !== nothing
        @assert length(item) == nrow(ds) "length of item values must be the same as the number of rows"
    elseif item isa SubDatasetColumn || item isa DatasetColumn
        item = __!(item)
    elseif item isa ColumnIndex
        item = _columns(ds)[index(ds)[item]]
    end
    colsidx = index(ds)[cols]
    idx = Ref{Int}(0)
    colnames_pa = allowmissing(PooledArray(names(ds, colsidx)))
    push!(colnames_pa, missing)
    missref = get(colnames_pa.invpool, missing, 0)
    init0 = fill(missref, nrow(ds))
    if item === nothing
        if threads
            mapreduce(identity, (x,y)->hp_op_for_findfirst!(x,y,f,idx, missref), view(_columns(ds),colsidx), init = init0)
        else
            mapreduce(identity, (x,y)->_op_for_findfirst!(x,y,f,idx, missref), view(_columns(ds),colsidx), init = init0)
        end
    else
        if threads
            mapreduce(identity, (x,y)->hp_op_for_findfirst!(x,y,item,idx, missref, eq), view(_columns(ds),colsidx), init = init0)
        else
            mapreduce(identity, (x,y)->_op_for_findfirst!(x,y,item,idx, missref, eq), view(_columns(ds),colsidx), init = init0)
        end
    end
    colnames_pa.refs = init0
    colnames_pa
end

function row_findlast(ds::AbstractDataset, f, cols = names(ds, Union{Missing, Number}); item::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex, Nothing} = nothing, threads = true, eq = isequal)
    if !(item isa ColumnIndex) && item !== nothing
        @assert length(item) == nrow(ds) "length of item values must be the same as the number of rows"
    elseif item isa SubDatasetColumn || item isa DatasetColumn
        item = __!(item)
    elseif item isa ColumnIndex
        item = _columns(ds)[index(ds)[item]]
    end
    colsidx = index(ds)[cols]
    idx = Ref{Int}(0)
    colnames_pa = allowmissing(PooledArray(names(ds, colsidx)))
    push!(colnames_pa, missing)
    missref = get(colnames_pa.invpool, missing, 0)
    init0 = fill(missref, nrow(ds))
    if item === nothing
        if threads
            mapreduce(identity, (x,y)->hp_op_for_findlast!(x,y,f,idx, missref), view(_columns(ds),colsidx), init = init0)
        else
            mapreduce(identity, (x,y)->_op_for_findlast!(x,y,f,idx, missref), view(_columns(ds),colsidx), init = init0)
        end
    else
        if threads
            mapreduce(identity, (x,y)->hp_op_for_findlast!(x,y,item,idx, missref, eq), view(_columns(ds),colsidx), init = init0)
        else
            mapreduce(identity, (x,y)->_op_for_findlast!(x,y,item,idx, missref, eq), view(_columns(ds),colsidx), init = init0)
        end
    end
    colnames_pa.refs = init0
    colnames_pa
end

function _op_for_in!(x,y,x1,eq)
    for i in 1:length(x)
        !x[i] ? x[i] = eq(x1[i], y[i]) : nothing
    end
    x
end
function hp_op_for_in!(x,y,x1,eq)
    Threads.@threads for i in 1:length(x)
        !x[i] ? x[i] = eq(x1[i], y[i]) : nothing
    end
    x
end

function row_in(ds::AbstractDataset, collections, items::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex}; threads = true, eq = isequal)
    if !(items isa ColumnIndex)
        @assert length(items) == nrow(ds) "length of item values must be the same as the number of rows"
    end
    colsidx = index(ds)[collections]
    if items isa SubDatasetColumn || items isa DatasetColumn
        items = __!(items)
    end
    if items isa ColumnIndex
        items = _columns(ds)[index(ds)[items]]
    end
    init0 = zeros(Bool, nrow(ds))
    if threads 
        mapreduce(identity, (x,y)->hp_op_for_in!(x,y,items,eq), view(_columns(ds),colsidx), init = init0)
    else
        mapreduce(identity, (x,y)->_op_for_in!(x,y,items,eq), view(_columns(ds),colsidx), init = init0)
    end

end

function _op_for_select!(x, y, colselector, dsnames, idx)
    idx[] += 1
    for i in 1:length(x)
        if isequal(colselector[i], dsnames[idx[]])
            x[i] = y[i]
        end
    end
    x
end
function hp_op_for_select!(x, y, colselector, dsnames, idx)
    idx[] += 1
    Threads.@threads for i in 1:length(x)
        if isequal(colselector[i], dsnames[idx[]])
            x[i] = y[i]
        end
    end
    x
end


function row_select(ds::AbstractDataset, cols, colselector::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex}; threads = true)
    if !(colselector isa ColumnIndex)
        @assert length(colselector) == nrow(ds) "to pick values of selected columns in each row, the length of the column names and the number of rows must match, i.e. the lenght of the vector passed as `by` must be $(nrow(ds))."
    end
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    idx = Ref{Int}(0)
    if colselector isa SubDatasetColumn || colselector isa DatasetColumn
        colselector = __!(colselector)
    end
    if colselector isa ColumnIndex
        colselector = _columns(ds)[index(ds)[colselector]]
    end
    if eltype(colselector) <: Union{Missing, Symbol}
        nnames = Symbol.(names(ds, colsidx))
    elseif eltype(colselector) <: Union{Missing, AbstractString}
        nnames = names(ds, colsidx)
    elseif eltype(colselector) <: Union{Missing, Integer}
        nnames = 1:length(colsidx)
    end
    init0 = missings(CT, nrow(ds))
    if threads
        mapreduce(identity, (x,y)->hp_op_for_select!(x,y,colselector,nnames, idx), view(_columns(ds),colsidx), init = init0)
    else
        mapreduce(identity, (x,y)->_op_for_select!(x,y,colselector,nnames, idx), view(_columns(ds),colsidx), init = init0)
    end
end

function _op_for_fill!(x,y,f)
    y .= ifelse.(f.(y), x, y)
    x
end
function hp_op_for_fill!(x,y,f)
  Threads.@threads for i in 1:length(x)
     y[i] = ifelse(f(y[i]), x[i], y[i])
  end
   x
end
function _op_for_fill_roll!(x,y,f)
    y .= ifelse.(f.(y), x, y)
    y
end
function hp_op_for_fill_roll!(x,y,f)
  Threads.@threads for i in 1:length(x)
     y[i] = ifelse(f(y[i]), x[i], y[i])
  end
  y
end


# rolling = true, means fill a column an use its value for next filling
function row_fill!(ds::AbstractDataset, cols, val::Union{AbstractVector, DatasetColumn, SubDatasetColumn, ColumnIndex}; f = ismissing, threads = true, rolling = false)
    if !(val isa ColumnIndex)
        @assert length(val) == nrow(ds) "to fill values in each row, the length of passed values and the number of rows must match."
    end
    colsidx = index(ds)[cols]
    if val isa SubDatasetColumn || val isa DatasetColumn
        val = __!(val)
    end
    if val isa ColumnIndex
        val = _columns(ds)[index(ds)[val]]
    end
    if threads
        if rolling
            mapreduce(identity, (x,y)->hp_op_for_fill_roll!(x,y, f), view(_columns(ds),colsidx), init = val)
        else
            mapreduce(identity, (x,y)->hp_op_for_fill!(x,y, f), view(_columns(ds),colsidx), init = val)
        end
    else
        if rolling
            mapreduce(identity, (x,y)->_op_for_fill_roll!(x,y, f), view(_columns(ds),colsidx), init = val)
        else
            mapreduce(identity, (x,y)->_op_for_fill!(x,y, f), view(_columns(ds),colsidx), init = val)
        end
    end
    any(index(parent(ds)).sortedcols .∈ Ref(IMD.parentcols.(Ref(IMD.index(ds)), cols))) && _reset_grouping_info!(parent(ds))
    _modified(_attributes(parent(ds)))
    ds
end


function _op_for_coalesce!(x, y)
    if all(!ismissing, x)
        x
    else
        x .= ifelse.(ismissing.(x), y, x)
    end
end

function row_coalesce(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))

    init0 = fill!(Vector{Union{Missing, CT}}(undef, size(ds,1)), missing)
    mapreduce(identity, _op_for_coalesce!, view(_columns(ds),colsidx), init = init0)
end

function row_mean(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    row_sum(ds, f, cols) ./ row_count(ds, x -> !ismissing(x), cols)
end
row_mean(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_mean(ds, identity, cols)

# TODO not safe if the first column is Vector{Missing}

function row_minimum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _op_for_min!(x, y) = x .= _min_fun.(x, f.(y))
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemax(T))
    mapreduce(identity, _op_for_min!, view(_columns(ds),colsidx), init = init0)
end
row_minimum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_minimum(ds, identity, cols)

# TODO not safe if the first column is Vector{Missing}

function row_maximum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _op_for_max!(x, y) = x .= _max_fun.(x, f.(y))
    # TODO the type of zeros after applying f???
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemin(T))
    mapreduce(identity, _op_for_max!, view(_columns(ds),colsidx), init = init0)
end
row_maximum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_maximum(ds, identity, cols)

function _op_for_argminmax!(x, y, f, vals, idx, missref)
    idx[] += 1
    for i in 1:length(x)
        if !ismissing(vals[i]) && isequal(vals[i], f(y[i])) && isequal(x[i], missref)
            x[i] = idx[]
        end
    end
    x
end
function hp_op_for_argminmax!(x, y, f, vals, idx, missref)
    idx[] += 1
    Threads.@threads for i in 1:length(x)
        if !ismissing(vals[i]) && isequal(vals[i], f(y[i])) && isequal(x[i], missref)
            x[i] = idx[]
        end
    end
    x
end


function row_argmin(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); threads = true)
    colsidx = index(ds)[cols]
    minvals = row_minimum(ds, f, cols)
    colnames_pa = allowmissing(PooledArray(names(ds, colsidx)))
    push!(colnames_pa, missing)
    missref = get(colnames_pa.invpool, missing, missing)
    init0 = fill(missref, nrow(ds))
    idx = Ref{Int}(0)
    if threads
        res = mapreduce(identity, (x,y)->hp_op_for_argminmax!(x,y, f, minvals, idx, missref), view(_columns(ds),colsidx), init = init0)

    else
        res = mapreduce(identity, (x,y)->_op_for_argminmax!(x,y, f, minvals, idx, missref), view(_columns(ds),colsidx), init = init0)
    end
    colnames_pa.refs = res
    colnames_pa
end
row_argmin(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); threads = true) = row_argmin(ds, identity, cols, threads = threads)

function row_argmax(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); threads = true)
    colsidx = index(ds)[cols]
    maxvals = row_maximum(ds, f, cols)
    colnames_pa = allowmissing(PooledArray(names(ds, colsidx)))
    push!(colnames_pa, missing)
    missref = get(colnames_pa.invpool, missing, missing)
    init0 = fill(missref, nrow(ds))
    idx = Ref{Int}(0)
    if threads
        res = mapreduce(identity, (x,y)->hp_op_for_argminmax!(x,y,f, maxvals, idx, missref), view(_columns(ds),colsidx), init = init0)
    else
        res = mapreduce(identity, (x,y)->_op_for_argminmax!(x,y,f, maxvals, idx, missref), view(_columns(ds),colsidx), init = init0)
    end
    colnames_pa.refs = res
    colnames_pa
end
row_argmax(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); threads = true) = row_argmax(ds, identity, cols, threads = threads)


# TODO better function for the first component of operator
function _row_wise_var(ss, sval, n, dof, T)
    res = Vector{T}(undef, length(ss))
    for i in 1:length(ss)
        if n[i] == 0
            res[i] = missing
        elseif n[i] == 1
            res[i] = zero(T)
        else
            res[i] = ss[i]/n[i] - (sval[i]/n[i])*(sval[i]/n[i])
            if dof
                res[i] = (n[i] * res[i])/(n[i]-1)
            end
        end
    end
    res
end

# TODO needs type stability
# TODO need abs2 for calculations
function row_var(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); dof = true)
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _sq_(x) = x^2
    ss = row_sum(ds, _sq_ ∘ f, cols)
    sval = row_sum(ds, f, cols)
    n = row_count(ds, x -> !ismissing(x), cols)
    T2 = Core.Compiler.return_type(/, (eltype(ss), eltype(n)))
    res = Vector{Union{Missing, T2}}(undef, length(ss))
    res .= ss ./ n .- (sval ./ n) .^ 2
    if dof
        res .= (n .* res) ./ (n .- 1)
        res .= ifelse.(n .== 1, missing, res)
    end
    res
    # _row_wise_var(ss, sval, n, dof, T)
end
row_var(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); dof = true) = row_var(ds, identity, cols, dof = dof)

function row_std(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); dof = true)
    sqrt.(row_var(ds, f, cols, dof = dof))
end
row_std(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); dof = true) = row_std(ds, identity, cols, dof = dof)

function _op_for_cumsum_skip!(x, y)
    x .= _add_sum.(x,y)
    y .= ifelse.(ismissing.(y), missing, x)
    x
end
_op_for_cumsum_ignore!(x, y) = y .= _add_sum.(x, y)


function row_cumsum!(ds::Dataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    if T <: Union{Missing, INTEGERS}
        T <: Union{Missing, Base.SmallSigned}
        T = T <: Union{Missing, Base.SmallSigned, Bool} ? Union{Int, Missing} : T
        T = T <: Union{Missing, Base.SmallUnsigned} ?  Union{Missing, UInt} : T
    end
    for i in colsidx
        if eltype(ds[!, i]) >: Missing
            _columns(ds)[i] = convert(Vector{Union{Missing, T}}, _columns(ds)[i])
        else
            _columns(ds)[i] = convert(Vector{T}, _columns(ds)[i])
        end
    end
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    if missings == :ignore
        mapreduce(identity,  _op_for_cumsum_ignore!, view(_columns(ds),colsidx), init = init0)
    elseif missings == :skip
        mapreduce(identity, _op_for_cumsum_skip!, view(_columns(ds),colsidx), init = init0)
    else
        throw(ArgumentError("`missings` can be either `:ignore` or `:skip`"))
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    ds
end
# row_cumsum!(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_cumsum!(identity, ds, cols)

function row_cumsum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    dscopy = copy(ds)
    row_cumsum!(dscopy, cols, missings = missings)
    dscopy
end

function _op_for_cumprod_skip!(x, y)
    x .= _mul_prod.(x,y)
    y .= ifelse.(ismissing.(y), missing, x)
    x
end
_op_for_cumprod_ignore!(x, y) = y .= _mul_prod.(x, y)

function row_cumprod!(ds::Dataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    for i in colsidx
        if eltype(ds[!, i]) >: Missing
            _columns(ds)[i] = convert(Vector{Union{Missing, T}}, _columns(ds)[i])
        else
            _columns(ds)[i] = convert(Vector{T}, _columns(ds)[i])
        end
    end
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : one(T))
    if missings == :ignore
        mapreduce(identity, _op_for_cumprod_ignore!, view(_columns(ds),colsidx), init = init0)
    elseif missings == :skip
        mapreduce(identity, _op_for_cumprod_skip!, view(_columns(ds),colsidx), init = init0)
    else
        throw(ArgumentError("`missings` can be either `:ignore` or `:skip`"))
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    ds
end

function row_cumprod(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    dscopy = copy(ds)
    row_cumprod!(dscopy, cols; missings = missings)
    dscopy
end


function _op_for_cummin_skip!(x, y)
    x .= _min_fun.(x,y)
    y .= ifelse.(ismissing.(y), missing, x)
    x
end
_op_for_cummin_ignore!(x, y) = y .= _min_fun.(x, y)


function row_cummin!(ds::Dataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    for i in colsidx
        if eltype(ds[!, i]) >: Missing
            _columns(ds)[i] = convert(Vector{Union{Missing, T}}, _columns(ds)[i])
        else
            _columns(ds)[i] = convert(Vector{T}, _columns(ds)[i])
        end
    end
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    if missings == :ignore
        mapreduce(identity,  _op_for_cummin_ignore!, view(_columns(ds),colsidx), init = init0)
    elseif missings == :skip
        mapreduce(identity, _op_for_cummin_skip!, view(_columns(ds),colsidx), init = init0)
    else
        throw(ArgumentError("`missings` can be either `:ignore` or `:skip`"))
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    ds
end
# row_cumsum!(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_cumsum!(identity, ds, cols)

function row_cummin(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    dscopy = copy(ds)
    row_cummin!(dscopy, cols, missings = missings)
    dscopy
end

function _op_for_cummax_skip!(x, y)
    x .= _max_fun.(x,y)
    y .= ifelse.(ismissing.(y), missing, x)
    x
end
_op_for_cummax_ignore!(x, y) = y .= _max_fun.(x, y)


function row_cummax!(ds::Dataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    for i in colsidx
        if eltype(ds[!, i]) >: Missing
            _columns(ds)[i] = convert(Vector{Union{Missing, T}}, _columns(ds)[i])
        else
            _columns(ds)[i] = convert(Vector{T}, _columns(ds)[i])
        end
    end
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    if missings == :ignore
        mapreduce(identity,  _op_for_cummax_ignore!, view(_columns(ds),colsidx), init = init0)
    elseif missings == :skip
        mapreduce(identity, _op_for_cummax_skip!, view(_columns(ds),colsidx), init = init0)
    else
        throw(ArgumentError("`missings` can be either `:ignore` or `:skip`"))
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    ds
end
# row_cumsum!(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_cumsum!(identity, ds, cols)

function row_cummax(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); missings = :ignore)
    dscopy = copy(ds)
    row_cummax!(dscopy, cols, missings = missings)
    dscopy
end


function row_stdze!(ds::Dataset , cols = names(ds, Union{Missing, Number}))
    meandata = row_mean(ds, cols)
    stddata = row_std(ds, cols)
    _stdze_fun(x) = ifelse.(isequal.(stddata, 0), missing, (x .- meandata) ./ stddata)
    colsidx = index(ds)[cols]

    for i in 1:length(colsidx)
        _columns(ds)[colsidx[i]] = _stdze_fun(_columns(ds)[colsidx[i]])
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
end


function row_stdze(ds::AbstractDataset , cols = names(ds, Union{Missing, Number}))
    dscopy = copy(ds)
    row_stdze!(dscopy, cols)
    dscopy
end

function row_sort!(ds::Dataset, cols = names(ds, Union{Missing, Number}); kwargs...)
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, eachcol(ds)[colsidx])
    m = Matrix{T}(ds[!, colsidx])
    sort!(m; dims = 2, kwargs...)
    for i in 1:length(colsidx)
        _columns(ds)[colsidx[i]] = m[:, i]
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    ds
end

function row_sort(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); kwargs...)
    dscopy = copy(ds)
    row_sort!(dscopy, cols; kwargs...)
    dscopy
end

function _op_for_issorted!(x, y, res, lt)
    res .&= .!lt.(y, x)
    y
end
function _op_for_issorted_rev!(x, y, res, lt)
    res .&= .!lt.(x, y)
    y
end

function row_issorted(ds::AbstractDataset, cols; rev = false, lt = isless)
    colsidx = index(ds)[cols]
    init0 = ones(Bool, nrow(ds))
    if rev
        mapreduce(identity, (x, y)->_op_for_issorted_rev!(x, y, init0, lt), view(_columns(ds),colsidx))
    else
        mapreduce(identity, (x, y)->_op_for_issorted!(x, y, init0, lt), view(_columns(ds),colsidx))
    end
    init0
end

# TODO is it possible to have a faster row_count_unique??
function _fill_prehashed!(prehashed, y, f, n, j)
    @views copy!(prehashed[:, j] , _Prehashed.(hash.(f.(y))))
end

function _fill_dict_and_add!(init0, dict, prehashed, n, p)
    for i in 1:n
        for j in 1:p
            if !haskey(dict, prehashed[i, j])
                get!(dict, prehashed[i, j], nothing)
                init0[i] += 1
            end
        end
        empty!(dict)
    end
end

function row_nunique(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); count_missing = true)
    colsidx = index(ds)[cols]
    prehashed = Matrix{_Prehashed}(undef, size(ds,1), length(colsidx))
    allcols = view(_columns(ds),colsidx)

    for j in 1:size(prehashed,2)
        _fill_prehashed!(prehashed, allcols[j], f, size(ds,1), j)
    end

    init0 = zeros(Int32, size(ds,1))
    dict = Dict{_Prehashed, Nothing}()
    _fill_dict_and_add!(init0, dict, prehashed, size(ds,1), length(colsidx))
    if count_missing
        return init0
    else
        return init0 .- row_any(ds, ismissing, cols)
    end
end
row_nunique(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); count_missing = true) = row_nunique(ds, identity, cols; count_missing = count_missing)

function row_hash(ds::AbstractDataset, f::Function, cols = :)
    colsidx = index(ds)[cols]
    _op_hash(x, y) = x .= hash.(f.(y), x)
    mapreduce(identity, _op_hash, view(_columns(ds),colsidx), init = zeros(UInt64, size(ds,1)))
end
row_hash(ds::AbstractDataset, cols = :) = row_hash(ds, identity, cols)

function _fill_col!(inmat, column, rows, j)
    for i in 1:length(rows)
        inmat[j, i] = column[rows[i]]
    end
end
function _fill_matrix!(inmat, all_data, rows, cols)
    for j in 1:length(cols)
        _fill_col!(inmat, all_data[j], rows, j)
    end
end

function row_generic(ds::AbstractDataset, f::Function, cols::MultiColumnIndex)
    colsidx = index(ds)[cols]
    if length(colsidx) == 2
        try
            allowmissing(f.(_columns(ds)[colsidx[1]], _columns(ds)[colsidx[2]]))
        catch e
            if e isa MethodError
                _row_generic(ds, f, colsidx)
            else
                rethrow(e)
            end
        end
    else
        _row_generic(ds, f, colsidx)
    end
end
function _row_generic(ds::AbstractDataset, f::Function, colsidx)
    T = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    inmat = Matrix{T}(undef, length(colsidx), min(1000, nrow(ds)))

    all_data = view(_columns(ds), colsidx)
    _fill_matrix!(inmat, all_data, 1:min(1000, nrow(ds)), colsidx)
    res_temp = allowmissing(f.(eachcol(inmat)))
    if !(typeof(res_temp) <:  AbstractVector)
        throw(ArgumentError("output of `f` must be a vector"))
    end

    # if length(res_temp[1]) > 1
    #     throw(ArgumentError("The matrix output is not supported"))
    #     res = similar(res_temp, nrow(ds), size(res_temp,2))
    # elseif length(res_temp[1]) == 1
    res = similar(res_temp, nrow(ds))
    # else
        # throw(ArgumentError("the result cannot be with zero dimension"))

    if nrow(ds)>1000
        if size(res, 2) == 1
            view(res, 1:1000) .= res_temp
            _row_generic_vec!(res, ds, f, colsidx, Val(T))
        else
            view(res, 1:1000, :) .= res_temp
            _row_generic_mat!(res, ds, f, colsidx)
        end
    else
        return res_temp
    end
    return res
end

function _row_generic_vec!_barrier(res, f, inmat, all_data, chunck, colsidx)

    for i in 1:chunck
        t_st = i*1000 + 1
        i == chunck ? t_en = length(res) : t_en = (i+1)*1000
        _fill_matrix!(inmat, all_data, t_st:t_en, colsidx)
        for k in t_st:t_en
            res[k] = f(view(inmat, :, k - t_st + 1))
        end
    end
end

function _row_generic_vec!(res, ds, f, colsidx, ::Val{T}) where T
    all_data = view(_columns(ds), colsidx)
    chunck = div(length(res) - 1000, 1000)
    max_cz = length(res) - 1000 - (chunck - 1)* 1000
    inmat = Matrix{T}(undef, length(colsidx), max_cz)
    # make sure that the variable inside the loop are not the same as the out of scope one
    # for i in 1:chunck
    #     t_st = i*1000 + 1
    #     i == chunck ? t_en = length(res) : t_en = (i+1)*1000
    #     _fill_matrix!(inmat, all_data, t_st:t_en, colsidx)
    #     for k in t_st:t_en
    #         res[k] = f(view(inmat, k - t_st + 1, :))
    #     end
    # end
    _row_generic_vec!_barrier(res, f, inmat, all_data, chunck, colsidx)
end
