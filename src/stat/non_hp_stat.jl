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
function topk_sort!(v::AbstractVector, lo::Integer, hi::Integer, lt_fun)
    @inbounds for i = lo+1:hi
        j = i
        x = v[i]
        while j > lo
            if lt_fun(x, v[j-1])
                v[j] = v[j-1]
                j -= 1
                continue
            end
            break
        end
        v[j] = x
    end
    return v
end
function topk_sort_permute!(v::AbstractVector, perm::AbstractVector, lo::Integer, hi::Integer, lt_fun)
    @inbounds for i = lo+1:hi
        j = i
        x = v[i]
        y = perm[i]
        while j > lo
            if lt_fun(x, v[j-1])
                v[j] = v[j-1]
                perm[j] = perm[j-1]
                j -= 1
                continue
            end
            break
        end
        v[j] = x
        perm[j] = y
    end
    return v
end

function initiate_topk_res!(res, x, by)
    cnt = 1
    idx = 1
    @inbounds for i in 1:length(x)
        idx = i
        if !ismissing(by(x[i]))
            res[cnt] = x[i]
            cnt += 1
            if cnt > length(res)
                break
            end
        end
    end
    idx, cnt - 1
end
function initiate_topk_res_perm!(perm, res, x, by; offset=0)
    cnt = 1
    idx = 1
    @inbounds for i in 1:length(x)
        idx = i
        if !ismissing(by(x[i]))
            res[cnt] = x[i]
            perm[cnt] = i + offset
            cnt += 1
            if cnt > length(res)
                break
            end
        end
    end
    idx, cnt - 1
end

Base.@propagate_inbounds function insert_fixed_sorted!(x, item, lt_fun)
    if !lt_fun(item, x[end])
        return
    end
    x[end] = item
    j = length(x)
    while j > 1
        if lt_fun(item, x[j-1])
            x[j] = x[j-1]
            j -= 1
            continue
        end
        break
    end
    x[j] = item
    nothing
end
# TODO we do not need x, this is just easier to implement, later we may fix this
Base.@propagate_inbounds function insert_fixed_sorted_perm!(perm, x, idx, item, lt_fun)
    if !lt_fun(item, x[end])
        return
    end
    x[end] = item
    perm[end] = idx
    j = length(x)
    while j > 1
        if lt_fun(item, x[j-1])
            x[j] = x[j-1]
            perm[j] = perm[j-1]
            j -= 1
            continue
        end
        break
    end
    x[j] = item
    perm[j] = idx
    nothing
end
Base.@propagate_inbounds function topk_vals(x::AbstractVector{T}, k::Int, lt_fun::F, by) where {T} where {F}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    all(ismissing, x) && return Union{Missing,T}[missing]
    res = Vector{T}(undef, k)
    idx, cnt = initiate_topk_res!(res, x, by)
    topk_sort!(res, 1, cnt, lt_fun)
    for i in idx+1:length(x)
        if !ismissing(by(x[i]))
            insert_fixed_sorted!(res, x[i], lt_fun)
            cnt += 1
        end
    end
    if cnt < k
        allowmissing(resize!(res, cnt))
    else
        allowmissing(res)
    end
end

# ktop permutation
#TODO should we return [missing] or Int[] when all elements are missings?
Base.@propagate_inbounds function topk_perm(x::AbstractVector{T}, k::Int, lt_fun::F, by) where {T} where {F}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    all(ismissing, x) && return Union{Missing,Int}[missing]
    res = Vector{T}(undef, k)
    perm = zeros(Int, k)
    idx, cnt = initiate_topk_res_perm!(perm, res, x, by)
    topk_sort_permute!(res, perm, 1, cnt, lt_fun)
    for i in idx+1:length(x)
        if !ismissing(by(x[i]))
            insert_fixed_sorted_perm!(perm, res, i, x[i], lt_fun)
            cnt += 1
        end
    end
    if cnt < k
        allowmissing(resize!(perm, cnt))
    else
        allowmissing(perm)
    end
end

"""
    topk(x, k; rev = false, lt = <, by = identity, threads = false)

Return upto `k` largest nonmissing elements of `x`. When `rev = true` it returns upto `k` smallest nonmissing elements of `x`. When all elements are missing, the function returns `[missing]`. The `by` keyword lets you provide a function that will be applied to each element before comparison; the `lt` keyword allows providing a custom "less than" function (note that for every x and y, only one of `lt(x,y)` and `lt(y,x)` can return true)

Also see [`topkperm`](@ref), [`partialsort`](@ref)

# Example
```jldoctest
julia> x = [10, -11, missing, 1, 0]
5-element Vector{Union{Missing, Int64}}:
  10
 -11
    missing
   1
   0

julia> topk(x, 3)
3-element Vector{Union{Missing, Int64}}:
 10
  1
  0

julia> topk(x, 3, by = abs)
3-element Vector{Union{Missing, Int64}}:
 -11
  10
   1

julia> topk(x, 3, by = abs, rev = true)
3-element Vector{Union{Missing, Int64}}:
  0
  1
 10
```
"""
function topk(x::AbstractVector, k::Int; rev::Bool=false, lt=<, by=identity, threads=false)
    @assert firstindex(x) == 1 "topk only supports 1-based indexing"
    if threads && length(x) > Threads.nthreads()
        if rev
            hp_topk_vals(x, k, (y1, y2) -> lt(by(y1), by(y2)), by)
        else
            hp_topk_vals(x, k, (y1, y2) -> lt(by(y2), by(y1)), by)
        end
    else
        if rev
            topk_vals(x, k, (y1, y2) -> lt(by(y1), by(y2)), by)
        else
            topk_vals(x, k, (y1, y2) -> lt(by(y2), by(y1)), by)
        end
    end
end
"""
    topkperm(x, k; rev = false, lt = <, by = identity, threads = false)

Return the indices of upto `k` largest nonmissing elements of `x`. When `rev = true` it returns the indices of upto `k` smallest nonmissing elements of `x`. When all elements are missing, the function returns `[missing]`. The `by` keyword lets you provide a function that will be applied to each element before comparison; the `lt` keyword allows providing a custom "less than" function (note that for every x and y, only one of `lt(x,y)` and `lt(y,x)` can return true)

Also see [`topk`](@ref), [`partialsortperm`](@ref)

# Examples
```jldoctest
julia> x = rand(1000);

julia> topkperm(x, 10)
10-element Vector{Union{Missing, Int64}}:
 711
 470
 291
 401
 927
 124
 949
 164
 216
 948

julia> topkperm(x, 10, threads = true)
10-element Vector{Union{Missing, Int64}}:
 711
 470
 291
 401
 927
 124
 949
 164
 216
 948

julia> topkperm(x, 10, threads = true, rev = true)
10-element Vector{Union{Missing, Int64}}:
 207
 351
 497
 768
 135
 599
 608
 798
 675
 648
```
"""
function topkperm(x::AbstractVector, k::Int; rev::Bool=false, lt=<, by=identity, threads=false)
    @assert firstindex(x) == 1 "topkperm only supports 1-based indexing"
    if threads && length(x) > Threads.nthreads()
        if rev
            hp_topk_perm(x, k, (y1, y2) -> lt(by(y1), by(y2)), by)
        else
            hp_topk_perm(x, k, (y1, y2) -> lt(by(y2), by(y1)), by)
        end
    else
        if rev
            topk_perm(x, k, (y1, y2) -> lt(by(y1), by(y2)), by)
        else
            topk_perm(x, k, (y1, y2) -> lt(by(y2), by(y1)), by)
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
(bfill, bfill!)

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
