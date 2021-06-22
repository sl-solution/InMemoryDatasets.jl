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
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zero(CT)))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _op_for_sum!(x, y) = x .= _add_sum.(x, f.(y))
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    mapreduce(identity, _op_for_sum!, view(getfield(ds, :columns),colsidx), init = init0)
end
row_sum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_sum(ds, identity, cols)


function row_prod(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zero(CT)))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _op_for_prod!(x, y) = x .= _mul_prod.(x, f.(y))
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : one(T))
    mapreduce(identity, _op_for_prod!, view(getfield(ds, :columns),colsidx), init = init0)
end
row_prod(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_prod(ds, identity, cols)


function row_count(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    _op_for_count!(x, y) = x .+= (_bool(f).(y))
    mapreduce(identity, _op_for_count!, view(getfield(ds, :columns),colsidx), init = zeros(Int32, size(ds,1)))
end
row_count(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_count(ds, x->true, cols)

function row_any(ds::AbstractDataset, f::Function, cols = :)
    colsidx = index(ds)[cols]
    _op_bool_add(x::Bool,y::Bool) = x | y ? true : false
    op_for_any!(x,y) = x .= _op_bool_add.(x, _bool(f).(y))
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, op_for_any!, view(getfield(ds, :columns),colsidx), init = zeros(Bool, size(ds,1)))
end
row_any(ds::AbstractDataset, cols = :) = row_any(ds, isequal(true), cols)

function row_all(ds::AbstractDataset, f::Function, cols = :)
    colsidx = index(ds)[cols]
    _op_bool_mult(x::Bool,y::Bool) = x & y ? true : false
    op_for_all!(x,y) = x .= _op_bool_mult.(x, _bool(f).(y))
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce(identity, op_for_all!, view(getfield(ds, :columns),colsidx), init = ones(Bool, size(ds,1)))
end
row_all(ds::AbstractDataset, cols = :) = row_all(ds, isequal(true), cols)


function row_mean(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    row_sum(ds, f, cols) ./ row_count(ds, x -> !ismissing(x), cols)
end
row_mean(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_mean(ds, identity, cols)

# TODO not safe if the first column is Vector{Missing}

function row_minimum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    # since zero(Date) is Day(0)
    T = typeof(f(zeros(CT)[1]))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _op_for_min!(x, y) = x .= _min_fun.(x, f.(y))
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemax(T))
    mapreduce(identity, _op_for_min!, view(getfield(ds, :columns),colsidx), init = init0)
end
row_minimum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_minimum(ds, identity, cols)

# TODO not safe if the first column is Vector{Missing}

function row_maximum(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zeros(CT)[1]))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _op_for_max!(x, y) = x .= _max_fun.(x, f.(y))
    # TODO the type of zeros after applying f???
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : typemin(T))
    mapreduce(identity, _op_for_max!, view(getfield(ds, :columns),colsidx), init = init0)
end
row_maximum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_maximum(ds, identity, cols)

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

function row_var(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); dof = true)
    colsidx = index(ds)[cols]
    CT = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    T = typeof(f(zero(CT)))
    if CT >: Missing
        T = Union{Missing, T}
    end
    _sq_(x) = x^2
    ss = row_sum(ds, _sq_ ∘ f, cols)
    sval = row_sum(ds, f, cols)
    n = row_count(ds, x -> !ismissing(x), cols)
    res = ss ./ n .- (sval ./ n) .^ 2
    if dof
        res .= (n .* res) ./ (n .- 1)
        res .= ifelse.(n .== 1, zero(T), res)
    end
    res
    # _row_wise_var(ss, sval, n, dof, T)
end
row_var(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); dof = true) = row_var(ds, identity, cols, dof = dof)

function row_std(ds::AbstractDataset, f::Function, cols = names(ds, Union{Missing, Number}); dof = true)
    sqrt.(row_var(ds, f, cols, dof = dof))
end
row_std(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); dof = true) = row_std(ds, identity, cols, dof = dof)

function row_cumsum!(ds::Dataset, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    for i in colsidx
        if eltype(ds[!, i]) >: Missing
            _columns(ds)[i] = convert(Vector{Union{Missing, T}}, _columns(ds)[i])
        else
            _columns(ds)[i] = convert(Vector{T}, _columns(ds)[i])
        end
    end
    _op_for_cumsum!(x, y) = y .= _add_sum.(x, y)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : zero(T))
    mapreduce(identity, _op_for_cumsum!, view(getfield(ds, :columns),colsidx), init = init0)
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    nothing
end
# row_cumsum!(ds::AbstractDataset, cols = names(ds, Union{Missing, Number})) = row_cumsum!(identity, ds, cols)

function row_cumsum(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}))
    dscopy = copy(ds)
    row_cumsum!(dscopy, cols)
    dscopy
end


function row_cumprod!(ds::Dataset, cols = names(ds, Union{Missing, Number}))
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, view(getfield(ds, :columns),colsidx))
    for i in colsidx
        if eltype(ds[!, i]) >: Missing
            _columns(ds)[i] = convert(Vector{Union{Missing, T}}, _columns(ds)[i])
        else
            _columns(ds)[i] = convert(Vector{T}, _columns(ds)[i])
        end
    end
    _op_for_cumprod!(x, y) = y .= _mul_prod.(x, y)
    init0 = fill!(Vector{T}(undef, size(ds,1)), T >: Missing ? missing : one(T))
    mapreduce(identity, _op_for_cumprod!, view(getfield(ds, :columns),colsidx), init = init0)
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    nothing
end

function row_cumprod(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}))
    dscopy = copy(ds)
    row_cumprod!(dscopy, cols)
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
        getfield(ds, :columns)[colsidx[i]] = m[:, i]
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
end

function row_sort(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); kwargs...)
    dscopy = copy(ds)
    row_sort!(dscopy, cols; kwargs...)
    dscopy
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
    allcols = view(getfield(ds, :columns),colsidx)

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
    mapreduce(identity, _op_hash, view(getfield(ds, :columns),colsidx), init = zeros(UInt64, size(ds,1)))
end
row_hash(ds::AbstractDataset, cols = :) = row_hash(ds, identity, cols)
