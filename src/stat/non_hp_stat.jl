# _stat_add_sum(x::T, y::S) where T where S = convert(promote_type(S,T), x + y)
_stat_add_sum(x, y) = Base.add_sum(x, y)
_stat_add_sum(x::Bool, y::Bool) = x + y
_stat_add_sum(x::Missing, y::Bool) = Int(y)
_stat_add_sum(x::Bool, y::Missing) = Int(x)
_stat_add_sum(x, ::Missing) = x
_stat_add_sum(::Missing, x) = x
_stat_add_sum(::Missing, ::Missing) = missing
_stat_mul_prod(x, y) = Base.mul_prod(x, y)
_stat_mul_prod(x, ::Missing) = x
_stat_mul_prod(::Missing, x) = x
_stat_mul_prod(::Missing, ::Missing) = missing
_stat_min_fun(x, y) = min(x, y)
_stat_min_fun(x, ::Missing) = x
_stat_min_fun(::Missing, y) = y
_stat_min_fun(::Missing, ::Missing) = missing
_stat_max_fun(x, y) = max(x, y)
_stat_max_fun(x, ::Missing) = x
_stat_max_fun(::Missing, y) = y
_stat_max_fun(::Missing, ::Missing) = missing
_stat_realXcY(x, y) = Statistics.realXcY(x, y)
_stat_realXcY(x, ::Missing) = x
_stat_realXcY(::Missing, y) = y
_stat_realXcY(::Missing, ::Missing) = missing
ISNAN(x::Any) = isnan(x)
ISNAN(::Missing) = false

_stat_bool(f) = x -> f(x)::Bool

_stat_ismissing(x::Any)::Int = 0
_stat_ismissing(::Missing)::Int = 1
_stat_notmissing(x::Any)::Int = 1
_stat_notmissing(::Missing)::Int = 0

"""
    lag(x, k; default = missing)
    lag!(x, k; default = missing)

Create a lag-k of the provided vector `x`. The output will be a vector the
same size as `x` (the input array).

`lag!` replace the input vector.

"""
(lag, lag!)

function lag(x::AbstractVector, k; default=missing)
    @assert firstindex(x) == 1 "lag only supports 1-based indexing"
    res = Vector{Union{promote_type(typeof(default), eltype(x)),Missing}}(undef, length(x))
    for i in 1:k
        @inbounds res[i] = default
    end
    for i in (k+1):length(x)
        @inbounds res[i] = x[i-k]
    end
    res
end

lag(x::AbstractVector; default=missing) = lag(x, 1; default=default)

function lag!(x::AbstractVector, k; default=missing)
    @assert firstindex(x) == 1 "lag! only supports 1-based indexing"
    @assert promote_type(typeof(default), eltype(x)) <: eltype(x) "`default` must be the same type as the element of the passed vector"
    for i in length(x):-1:(k+1)
        @inbounds x[i] = x[i-k]
    end
    for i in 1:k
        @inbounds x[i] = default
    end
    x
end
lag!(x::AbstractVector; default=missing) = lag!(x, 1; default=default)


"""
    lead(x, k; default = missing)
    lead!(x, k; default = missing)

Create a lead-k of the provided vector `x`. The output will be a vector the
same size as `x` (the input array).

`lead!` replace the input vector.

"""
(lead, lead!)

function lead(x::AbstractVector, k; default=missing)
    @assert firstindex(x) == 1 "lead only supports 1-based indexing"
    res = Vector{Union{promote_type(typeof(default), eltype(x)),Missing}}(undef, length(x))
    for i in 1:length(x)-k
        @inbounds res[i] = x[i+k]
    end
    for i in (length(x)-k+1):length(x)
        @inbounds res[i] = default
    end
    res
end
lead(x::AbstractVector; default=missing) = lead(x, 1; default=default)

function lead!(x::AbstractVector, k; default=missing)
    @assert firstindex(x) == 1 "lead! only supports 1-based indexing"
    @assert promote_type(typeof(default), eltype(x)) <: eltype(x) "`default` must be the same type as the element of the passed vector"
    for i in 1:length(x)-k
        @inbounds x[i] = x[i+k]
    end
    for i in (length(x)-k+1):length(x)
        @inbounds x[i] = default
    end
    x
end
lead!(x::AbstractVector; default=missing) = lead!(x, 1; default=default)

"""
    rescale(x,minx,maxx,minval,maxval)

Rescale x to run from minval and maxval, given x originaly runs from minx to maxx.
"""
function rescale(x, minx, maxx, minval, maxval)
    -(-maxx * minval + minx * maxval) / (maxx - minx) + (-minval + maxval) * x / (maxx - minx)
end
rescale(::Missing, minx, maxx, minval, maxval) = missing
rescale(x::Vector, minx, maxx, minval, maxval) = rescale.(x, minx, maxx, minval, maxval)
rescale(x, minx, maxx) = rescale(x, minx, maxx, 0.0, 1.0)

"""
    stdze(x)

Standardize an array. It returns missing for missing data points.
"""
function stdze(x)
    all(ismissing, x) && return x
    meandata = mean(x)
    vardata = var(x)
    (x .- meandata) ./ sqrt(vardata)
end

# this is manual simd version for max(min) function
function stat_maximum(f::typeof(identity), x::AbstractArray{T,1}; lo=1, hi=length(x)) where {T}
    all(ismissing, view(x, lo:hi)) && return missing
    _dmiss(x) = ismissing(x) ? typemin(nonmissingtype(T)) : x
    Base.mapreduce_impl(_dmiss, max, x, lo, hi)
end
function stat_maximum(f::F, x::AbstractArray{T,1}; lo=1, hi=length(x)) where {F,T}
    all(ismissing, view(x, lo:hi)) && return missing
    Base.mapreduce_impl(f, _stat_max_fun, x, lo, hi)
end
stat_maximum(x::AbstractArray{T,1}; lo=1, hi=length(x)) where {T} = stat_maximum(identity, x; lo=lo, hi=hi)

function _arg_minmax_barrier(x, minmaxval, f)::Int
    @inbounds for i in 1:length(x)
        isequal(f(x[i]), minmaxval) && return i
    end
end

function stat_findmax(f, x::AbstractArray{T,1}) where {T}
    isempty(x) && throw(ArgumentError("input vector cannot be empty"))
    maxval = stat_maximum(f, x)
    ismissing(maxval) && return (missing, missing)
    (maxval, _arg_minmax_barrier(x, maxval, f))
end
stat_findmax(x::AbstractArray{T,1}) where {T} = stat_findmax(identity, x)

function stat_minimum(f::typeof(identity), x::AbstractArray{T,1}; lo=1, hi=length(x)) where {T}
    all(ismissing, view(x, lo:hi)) && return missing
    @inline _dmiss(x) = ismissing(x) ? typemax(nonmissingtype(T)) : x
    Base.mapreduce_impl(_dmiss, min, x, lo, hi)
end
function stat_minimum(f::F, x::AbstractArray{T,1}; lo=1, hi=length(x)) where {F,T}
    all(ismissing, view(x, lo:hi)) && return missing
    Base.mapreduce_impl(f, _stat_min_fun, x, lo, hi)
end
stat_minimum(x::AbstractArray{T,1}; lo=1, hi=length(x)) where {T} = stat_minimum(identity, x; lo=lo, hi=hi)

function stat_findmin(f, x::AbstractArray{T,1}) where {T}
    isempty(x) && throw(ArgumentError("input vector cannot be empty"))
    minval = stat_minimum(f, x)
    (minval, _arg_minmax_barrier(x, minval, f))
end
stat_findmin(x::AbstractArray{T,1}) where {T} = stat_findmin(identity, x)


function stat_sum(f, x::AbstractArray{T,1}; lo=1, hi=length(x)) where {T<:Union{Missing,INTEGERS,FLOATS}}
    all(ismissing, view(x, lo:hi)) && return f(first(x))
    _dmiss(y) = ifelse(ismissing(f(y)), zero(T), f(y))
    Base.mapreduce_impl(_dmiss, _stat_add_sum, x, lo, hi)
end
stat_sum(x::AbstractArray{T,1}; lo=1, hi=length(x)) where {T<:Union{Missing,INTEGERS,FLOATS}} = stat_sum(identity, x; lo=lo, hi=hi)

# function stat_wsum(f, x::AbstractArray{Union{T,Missing},1}, w) where T
#     all(ismissing, x) && return missing
#     _dmiss(y) = ismissing(y[1])||ismissing(y[2]) ? zero(T) : (f(y[1])*y[2])
#     mapreduce(_dmiss, _stat_add_sum, zip(x,w))
# end
# stat_wsum(x::AbstractArray{Union{T,Missing},1}, w) where T  = stat_wsum(identity, x, w)
function stat_wsum(f, x::AbstractVector{T}, w::AbstractVector) where {T}
    all(ismissing, x) && return missing
    _dmiss(y) = ismissing(y[1]) || ismissing(y[2]) ? missing : (f(y[1]) * y[2])
    mapreduce(_dmiss, _stat_add_sum, zip(x, w))
end
stat_wsum(x::AbstractVector{T}, w::AbstractVector) where {T} = stat_wsum(identity, x, w)

function stat_mean(f, x::AbstractArray{T,1})::Union{Float64,Missing} where {T<:Union{Missing,INTEGERS,FLOATS}}
    length(x) == 1 && return f(first(x))
    sval = stat_sum(y -> f(y) * 1.0, x)
    n = mapreduce(!ismissing ∘ f, +, x)
    n == 0 ? missing : sval / n
end
stat_mean(x::AbstractArray{T,1}) where {T} = stat_mean(identity, x)

stat_cumsum_ignore(x::AbstractVector) = accumulate(_stat_add_sum, x)
stat_cumsum!_ignore(outx, inx::AbstractVector) = accumulate!(_stat_add_sum, outx, inx)
stat_cumprod_ignore(x::AbstractVector) = accumulate(_stat_mul_prod, x)
stat_cumprod!_ignore(outx, inx::AbstractVector) = accumulate!(_stat_mul_prod, outx, inx)
stat_cummin_ignore(x::AbstractVector) = accumulate(_stat_min_fun, x)
stat_cummin!_ignore(outx, inx::AbstractVector) = accumulate!(_stat_min_fun, outx, inx)
stat_cummax_ignore(x::AbstractVector) = accumulate(_stat_max_fun, x)
stat_cummax!_ignore(outx, inx::AbstractVector) = accumulate!(_stat_max_fun, outx, inx)

function stat_cumsum_skip(x::AbstractVector)
    locmiss = ismissing.(x)
    res = stat_cumsum_ignore(x)
    if sum(locmiss) > 0
        res[locmiss] .= missing
    end
    res
end

function stat_cumsum!_skip(outx, inx::AbstractVector)
    locmiss = ismissing.(inx)
    stat_cumsum!_ignore(outx, inx)
    if sum(locmiss) > 0
        outx[locmiss] .= missing
    end
    outx
end
function stat_cumprod_skip(x::AbstractVector)
    locmiss = ismissing.(x)
    res = stat_cumprod_ignore(x)
    if sum(locmiss) > 0
        res[locmiss] .= missing
    end
    res
end

function stat_cumprod!_skip(outx, inx::AbstractVector)
    locmiss = ismissing.(inx)
    stat_cumprod!_ignore(outx, inx)
    if sum(locmiss) > 0
        outx[locmiss] .= missing
    end
    outx
end

function stat_cummin_skip(x::AbstractVector)
    locmiss = ismissing.(x)
    res = stat_cummin_ignore(x)
    if sum(locmiss) > 0
        res[locmiss] .= missing
    end
    res
end

function stat_cummin!_skip(outx, inx::AbstractVector)
    locmiss = ismissing.(inx)
    stat_cummin!_ignore(outx, inx)
    if sum(locmiss) > 0
        outx[locmiss] .= missing
    end
    outx
end
function stat_cummax_skip(x::AbstractVector)
    locmiss = ismissing.(x)
    res = stat_cummax_ignore(x)
    if sum(locmiss) > 0
        res[locmiss] .= missing
    end
    res
end

function stat_cummax!_skip(outx, inx::AbstractVector)
    locmiss = ismissing.(inx)
    stat_cummax!_ignore(outx, inx)
    if sum(locmiss) > 0
        outx[locmiss] .= missing
    end
    outx
end

function stat_wmean(f, x::AbstractVector{T}, w::AbstractArray{S,1}) where {T} where {S}
    all(ismissing, x) && return missing
    _dmiss(y) = ismissing(y[1]) || ismissing(y[2]) ? zero(T) : (f(y[1]) * y[2])
    _dmiss2(y) = ismissing(y[1]) || ismissing(y[2]) ? zero(S) : y[2]
    _op(y1, y2) = _stat_add_sum.(y1, y2)
    _f(y) = (_dmiss(y), _dmiss2(y))
    sval, n = mapreduce(_f, _op, zip(x, w))
    n == 0 ? missing : sval / n
end
stat_wmean(x::AbstractVector{T}, w::AbstractArray{S,1}) where {T} where {S} = stat_wmean(identity, x, w)


function stat_var(f, x::AbstractArray{T,1}, dof=true)::Union{Float64,Missing} where {T<:Union{Missing,INTEGERS,FLOATS}}
    all(ismissing, x) && return missing
    # any(ISNAN, x) && return convert(eltype(x), NaN)
    # meanval = stat_mean(f, x)
    # n = mapreduce(!ismissing∘f, +, x)
    sval = stat_sum(y -> f(y) * 1.0, x)
    n = mapreduce(!ismissing ∘ f, +, x)
    meanval = n == 0 ? missing : sval / n

    ss = 0.0
    for i in 1:length(x)
        ss = _stat_add_sum(ss, abs2(f(x[i]) - meanval))
    end

    if n == 0
        return missing
    elseif n == 1 && dof
        return missing
    else
        return ss / (n - Int(dof))
    end
end

stat_var(x::AbstractArray{T,1}, dof=true) where {T} = stat_var(identity, x, dof)

function stat_std(f, x::AbstractArray{T,1}, dof=true)::Union{Float64,Missing} where {T<:Union{Missing,INTEGERS,FLOATS}}
    sqrt(stat_var(f, x, dof))
end
stat_std(x::AbstractArray{T,1}, dof=true) where {T} = stat_std(identity, x, dof)

function stat_median(v::AbstractArray{T,1}) where {T}
    isempty(v) && throw(ArgumentError("median of an empty array is undefined, $(repr(v))"))
    all(ismissing, v) && return missing
    (nonmissingtype(eltype(v)) <: AbstractFloat || nonmissingtype(eltype(v)) >: AbstractFloat) && any(ISNAN, v) && return convert(eltype(v), NaN)
    nmis::Int = mapreduce(ismissing, +, v)
    n = length(v) - nmis
    mid = div(1 + n, 2)
    if isodd(n)
        return middle(partialsort(v, mid))
    else
        m = partialsort(v, mid:mid+1)
        return middle(m[1], m[2])
    end
end

function stat_median!(v::AbstractArray{T,1}) where {T}
    isempty(v) && throw(ArgumentError("median of an empty array is undefined, $(repr(v))"))
    all(ismissing, v) && return missing
    (nonmissingtype(eltype(v)) <: AbstractFloat || nonmissingtype(eltype(v)) >: AbstractFloat) && any(ISNAN, v) && return convert(eltype(v), NaN)
    nmis::Int = mapreduce(ismissing, +, v)
    n = length(v) - nmis
    mid = div(1 + n, 2)
    if isodd(n)
        return middle(partialsort!(v, mid))
    else
        m = partialsort!(v, mid:mid+1)
        return middle(m[1], m[2])
    end
end

# finding k largest in an array with missing values
swap!(x, i, j) = x[i], x[j] = x[j], x[i]

TOPK_ISLESS(x,y) = isless(x, y)
TOPK_ISLESS(::Missing, y) = true
TOPK_ISLESS(x, ::Missing) = true
TOPK_ISLESS(::Missing, ::Missing) = false

function insert_fixed_sorted!(x, item, ord)
    if ord(item, x[end])
        x[end] = item
    else
       return
    end
    i = length(x) - 1
    while i > 0
        if ord((x[i+1]), (x[i]))
            swap!(x, i, i + 1)
            i -= 1
        else
            break
        end
    end
end
# TODO we do not need x, this is just easier to implement, later we may fix this
function insert_fixed_sorted_perm!(perm, x, idx, item, ord)
    if ord(item, x[end])
        x[end] = item
        perm[end] = idx
    else
        return
    end
    i = length(x) - 1
    while i > 0
        if ord((x[i+1]), (x[i]))
            swap!(x, i, i + 1)
            swap!(perm, i, i + 1)
            i -= 1
        else
            break
        end
    end
end


function k_largest(x::AbstractVector{T}, k::Int) where {T}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    k == 1 && return [maximum(identity, x)]
    all(ismissing, x) && return [missing]
    res = Vector{Union{Missing, T}}(undef, k)
    fill!(res, missing)
    cnt = 0
    for i in 1:length(x)
        if !ismissing(x[i])
            insert_fixed_sorted!(res, x[i], (y1, y2) -> TOPK_ISLESS(y2, y1))
            cnt += 1
        end
    end
    if cnt < k
        res[1:cnt]
    else
        res
    end
end

function k_smallest(x::AbstractVector{T}, k::Int) where {T}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    k == 1 && return [minimum(identity, x)]
    all(ismissing, x) && return [missing]
    res = Vector{Union{Missing, T}}(undef, k)
    fill!(res, missing)
    cnt = 0
    for i in 1:length(x)
        if !ismissing(x[i])
            insert_fixed_sorted!(res, x[i], (y1, y2) -> TOPK_ISLESS(y1, y2))
            cnt += 1
        end
    end
    if cnt < k
        res[1:cnt]
    else
        res
    end
end


# ktop permutation

function k_largest_perm(x::AbstractVector{T}, k::Int) where {T}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    k == 1 && return [maximum(identity, x)], [argmax(x)]
    all(ismissing, x) && return [missing], [missing]
    res = Vector{Union{Missing, T}}(undef, k)
    perm = zeros(Int, k)
    fill!(res, missing)
    cnt = 0
    for i in 1:length(x)
        if !ismissing(x[i])
            insert_fixed_sorted_perm!(perm, res, i, x[i], (y1, y2) -> TOPK_ISLESS(y2, y1))
            cnt += 1
        end
    end
    if cnt < k
        res[1:cnt], perm[1:cnt]
    else
        res, perm
    end
end

function k_smallest_perm(x::AbstractVector{T}, k::Int) where {T}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    k == 1 && return [minimum(identity, x)], [argmin(x)]
    all(ismissing, x) && return [missing], [missing]
    res = Vector{Union{Missing, T}}(undef, k)
    perm = zeros(Int, k)
    fill!(res, missing)
    cnt = 0
    for i in 1:length(x)
        if !ismissing(x[i])
            insert_fixed_sorted_perm!(perm, res, i, x[i], (y1, y2) -> TOPK_ISLESS(y1, y2))
            cnt += 1
        end
    end
    if cnt < k
        res[1:cnt], perm[1:cnt]
    else
        res, perm
    end
end


"""
    topk(x, k; rev = false, output_indices = false)

Return upto `k` largest nonmissing elements of `x`. When `rev = true` it returns upto `k` smallest nonmissing elements of `x`. When all elements are missing, the function returns `[missing]`.

If `output_indices = true`, the function returns the values and their indices.

> The `topk` function uses `isless` for comparing values
"""
function topk(x::AbstractVector, k::Int; rev::Bool=false, output_indices::Bool = false)
    @assert firstindex(x) == 1 "topk only supports 1-based indexing"
    if rev
        if output_indices
            k_smallest_perm(x, k)
        else
            k_smallest(x, k)
        end
    else
        if output_indices
            k_largest_perm(x, k)
        else
            k_largest(x, k)
        end
    end
end

"""
    ffill(x; [by = ismissing])
    ffill!(x; [by = ismissing])

Replace those elements of `x` which returns `true` when `by` is called on them with the previous element which calling `by` on it returns `false`.

`ffill!` modifies the input vector in-place

See also [`bfill`](@ref) and [`bfill!`](@ref)
"""
(ffill, ffill!)

function ffill!(x::AbstractVector; by=ismissing)
    @assert firstindex(x) == 1 "ffill!/ffill only support 1-based indexing"
    for i in 2:length(x)
        if by(x[i])
            x[i] = x[i-1]
        end
    end
    x
end
ffill(x; by=ismissing) = ffill!(copy(x), by=by)

"""
    bfill(x; [by = ismissing])
    bfill!(x; [by = ismissing])

Replace those elements of `x` which returns `true` when `by` is called on them with the next element which calling `by` on it returns `false`.

`bfill!` modifies the input vector in-place

See also [`ffill`](@ref) and [`ffill!`](@ref)
"""
function bfill!(x::AbstractVector; by=ismissing)
    @assert firstindex(x) == 1 "bfill!/bfill only support 1-based indexing"
    for i in length(x)-1:-1:1
        if by(x[i])
            x[i] = x[i+1]
        end
    end
    x
end
bfill(x, by=ismissing) = bfill!(copy(x), by=by)
