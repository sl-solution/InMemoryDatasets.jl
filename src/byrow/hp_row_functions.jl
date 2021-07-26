function hp_hash!(x, y ; f)
   Threads.@threads for i in 1:length(x)
       @inbounds x[i] = hash(f(y[i]), x[i])
   end
   x
end

function row_hash_hp(ds::AbstractDataset, f::Function, cols = :)
    colsidx = index(ds)[cols]
    _hp_op!(x, y; f = f) = x .= hp_hash!(x, y; f = f)
    mapreduce(identity, _hp_op!, view(_columns(ds),colsidx), init = zeros(UInt64, size(ds,1)))
end
row_hash_hp(ds::AbstractDataset, cols = :) = row_hash_hp(ds, f, cols)

function hp_sum!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _add_sum(x[i], f(y[i]))
    end
    x
end

function hp_row_sum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _hp_op_for_sum!(x, y; f = f) = x .= hp_sum!(x, y; f = f)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    mapreduce(identity, _hp_op_for_sum!, view(_columns(ds),colsidx), init = init0)
end
hp_row_sum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_sum(ds, identity, cols)

function hp_mult!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _mul_prod(x[i], f(y[i]))
    end
    x
end
function hp_row_prod(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _hp_op_for_prod!(x, y; f = f) = x .= hp_mult!(x, y; f = f)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : one(T))
    mapreduce(identity, _hp_op_for_prod!, view(_columns(ds),colsidx), init = init0)
end
hp_row_prod(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_prod(ds, identity, cols)

function hp_count!(x, y; f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] += _bool(f)(y[i])
    end
    x
end

function hp_row_count(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    _hp_op_for_count!(x, y; f = f) = x .= hp_count!(x, y; f = f)
    mapreduce(identity, _hp_op_for_count!, view(_columns(ds),colsidx), init = zeros(Int32, size(ds,1)))
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
    colsidx = index(ds)[cols]

    _hp_op_for_any!(x, y; f = f) = x .= hp_bool_add!(x, y; f = f)
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, _hp_op_for_any!, view(_columns(ds),colsidx), init = zeros(Bool, size(ds,1)))
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
    colsidx = index(ds)[cols]
    _hp_op_for_all!(x, y; f = f) = x .= hp_bool_mult!(x, y; f = f)
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, _hp_op_for_all!, view(_columns(ds),colsidx), init = ones(Bool, size(ds,1)))
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
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _hp_op_for_min!(x, y; f = f) = x .= hp_min!(x, y; f = f)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemax(T))
    mapreduce(identity, _hp_op_for_min!, view(_columns(ds),colsidx), init = init0)
end
hp_row_minimum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_minimum(ds, identity, cols)


function hp_max!(x, y; f = f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _max_fun(x[i], f(y[i]))
    end
    x
end


function hp_row_maximum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
    _hp_op_for_max!(x, y; f = f) = x .= hp_max!(x, y; f = f)
    # TODO the type of zeros after applying f???
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemin(T))
    mapreduce(identity, _hp_op_for_max!, view(_columns(ds),colsidx), init = init0)
end
hp_row_maximum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = hp_row_maximum(ds, identity, cols)

function hp_row_var(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); dof = true)
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    T = Core.Compiler.return_type(f, (CT,))
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
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, eachcol(ds)[colsidx])
    m = Matrix{T}(ds[!, colsidx])
    Threads.@threads for i in 1:size(m, 1)
        @views sort!(m[i, :], kwargs...)
    end
    # TODO no parallel is needed here to minimise memory
    for i in 1:length(colsidx)
        _columns(ds)[colsidx[i]] = m[:, i]
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    ds
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


function hp_row_generic(ds::AbstractDataset, f::Function, cols::MultiColumnIndex)
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
        _hp_row_generic(ds, f, colsidx)
    end
end


function _hp_row_generic(ds::AbstractDataset, f::Function, colsidx)
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
            _hp_row_generic_vec!(res, ds, f, colsidx, Val(T))
        else
            view(res, 1:1000, :) .= res_temp
            # not implemented yet
            _hp_row_generic_mat!(res, ds, f, colsidx)
        end
    else
        return res_temp
    end
    return res
end

function _hp_row_generic_vec!(res, ds, f, colsidx, ::Val{T}) where T
    nt = Threads.nthreads()
    loopsize = div(length(res) - 1000, 1000)
    all_data = view(_columns(ds), colsidx)
    if loopsize == 0
        st = 1001
        en = length(res)
        inmat = Matrix{T}(undef, length(colsidx), en - st + 1)
        _fill_matrix!(inmat, all_data, st:en, colsidx)
        view(res, st:en) .= f.(eachcol(inmat))
        return
    end
    max_cz = length(res) - 1000 - (loopsize - 1)*1000
    inmat_all = [Matrix{T}(undef, length(colsidx), max_cz) for i in 1:nt]
    # make sure that the variable inside the loop are not the same as the out of scope one
    Threads.@threads for i in 1:loopsize
        t_st = i*1000 + 1
        i == loopsize ? t_en = length(res) : t_en = (i+1)*1000
        _fill_matrix!(inmat_all[Threads.threadid()], all_data, t_st:t_en, colsidx)
        for k in t_st:t_en
            res[k] = f(view(inmat_all[Threads.threadid()], :, k - t_st + 1))
        end
    end
end
