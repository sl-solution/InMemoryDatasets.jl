function hp_hash!(x, y ; f)
   Threads.@threads for i in 1:length(x)
       @inbounds x[i] = hash(f(y[i]), x[i])
   end
   x
end

function row_hash_hp(ds::AbstractDataset, f::Function, cols = :)
    colsidx = col_index(ds)[cols]
    _hp_op!(x, y; f = f) = x .= hp_hash!(x, y; f = f)
    mapreduce(identity, _hp_op!, view(getfield(ds, :columns),colsidx), init = zeros(UInt64, size(ds,1)))
end
row_hash_hp(ds::AbstractDataset, cols = :) = row_hash_hp(ds, f, cols)

function hp_sum!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _add_sum(x[i], f(y[i]))
    end
    x
end

function hp_row_sum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = col_index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zero(CT)))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _hp_op_for_sum!(x, y; f = f) = x .= hp_sum!(x, y; f = f)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    mapreduce(identity, _hp_op_for_sum!, view(getfield(ds, :columns),colsidx), init = init0)
end
hp_row_sum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_sum(ds, identity, cols)

function hp_mult!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _mul_prod(x[i], f(y[i]))
    end
    x
end
function hp_row_prod(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = col_index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zero(CT)))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _hp_op_for_prod!(x, y; f = f) = x .= hp_mult!(x, y; f = f)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : one(T))
    mapreduce(identity, _hp_op_for_prod!, view(getfield(ds, :columns),colsidx), init = init0)
end
hp_row_prod(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_prod(ds, identity, cols)

function hp_count!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] += _bool(f)(y[i])
    end
    x
end

function hp_row_count(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = col_index(ds)[cols]
    _hp_op_for_count!(x, y; f = f) = x .= hp_count!(x, y; f = f)
    mapreduce(identity, _hp_op_for_count!, view(getfield(ds, :columns),colsidx), init = zeros(Int32, size(ds,1)))
end
hp_row_count(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_count(ds, x->true, cols)

_op_bool_add(x::Bool,y::Bool) = x | y ? true : false
function hp_bool_add!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _op_bool_add(x[i], _bool(f)(y[i]))
    end
    x
end

function hp_row_any(ds::AbstractDataset, f::Function, cols = :)
    colsidx = col_index(ds)[cols]

    _hp_op_for_any!(x, y; f = f) = x .= hp_bool_add!(x, y; f = f)
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, _hp_op_for_any!, view(getfield(ds, :columns),colsidx), init = zeros(Bool, size(ds,1)))
end
hp_row_any(ds::AbstractDataset, cols = :) = hp_row_any(ds, isequal(true), cols)

_op_bool_mult(x::Bool,y::Bool) = x & y ? true : false

function hp_bool_mult!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _op_bool_mult(x[i], _bool(f)(y[i]))
    end
    x
end

function hp_row_all(ds::AbstractDataset, f::Function, cols = :)
    colsidx = col_index(ds)[cols]
    _hp_op_for_all!(x, y; f = f) = x .= hp_bool_mult!(x, y; f = f)
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, _hp_op_for_all!, view(getfield(ds, :columns),colsidx), init = ones(Bool, size(ds,1)))
end
hp_row_all(ds::AbstractDataset, cols = :) = hp_row_all(ds, isequal(true), cols)


function hp_row_mean(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    hp_row_sum(ds, f, cols) ./ hp_row_count(ds, x -> !ismissing(x), cols)
end
hp_row_mean(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_mean(ds, identity, cols)

function hp_min!(x, y; f = f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _min_fun(x[i], f(y[i]))
    end
    x
end

function hp_row_minimum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = col_index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    # since zero(Date) is Day(0)
    T = typeof(f(zeros(CT)[1]))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _hp_op_for_min!(x, y; f = f) = x .= hp_min!(x, y; f = f)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemax(T))
    mapreduce(identity, _hp_op_for_min!, view(getfield(ds, :columns),colsidx), init = init0)
end
hp_row_minimum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_minimum(ds, identity, cols)


function hp_max!(x, y; f = f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _max_fun(x[i], f(y[i]))
    end
    x
end


function hp_row_maximum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = col_index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zeros(CT)[1]))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _hp_op_for_max!(x, y; f = f) = x .= hp_max!(x, y; f = f)
    # TODO the type of zeros after applying f???
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemin(T))
    mapreduce(identity, _hp_op_for_max!, view(getfield(ds, :columns),colsidx), init = init0)
end
hp_row_maximum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_maximum(ds, identity, cols)

function hp_row_var(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); dof = true)
    colsidx = col_index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zero(CT)))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _sq_(x) = x^2
    ss = hp_row_sum(ds, _sq_ ∘ f, cols)
    sval = hp_row_sum(ds, f, cols)
    n = hp_row_count(ds, x -> !ismissing(x), cols)
    res = ss ./ n .- (sval ./ n) .^ 2
    if dof
        res .= (n .* res) ./ (n .- 1)
        res .= ifelse.(n .== 1, zero(T), res)
    end
    res
    # _row_wise_var(ss, sval, n, dof, T)
end
hp_row_var(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); dof = true) = hp_row_var(ds, identity, cols, dof = dof)


function hp_row_std(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); dof = true)
    sqrt.(hp_row_var(ds, f, cols, dof = dof))
end
hp_row_std(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); dof = true) = hp_row_std(ds, identity, cols, dof = dof)


function hp_row_sort!(ds::Dataset, cols = names(ds, Union{Missing, Number}); kwargs...)
    colsidx = col_index(ds)[cols]
    T = mapreduce(eltype, promote_type, eachcol(ds)[colsidx])
    m = Matrix{T}(ds[!, colsidx])
    Threads.@threads for i in 1:size(m, 1)
        @views sort!(m[i, :], kwargs...)
    end
    # TODO no parallel is needed here to minimise memory
    for i in 1:length(colsidx)
        getfield(ds, :columns)[colsidx[i]] = m[:, i]
    end
end

"""
    row_sort!(ds::AbstractDataset[, cols]; kwargs...)
    sort `cols` in each row.
"""
function hp_row_sort(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); kwargs...)
    dscopy = copy(ds)
    hp_row_sort!(dscopy, cols; kwargs...)
    dscopy
end
