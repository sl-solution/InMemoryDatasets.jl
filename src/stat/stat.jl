maximum(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : threads ? hp_maximum(f, x) : stat_maximum(f, x)
maximum(f, x; threads = false) = Base.maximum(f, x)
maximum(x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType}  = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : threads ? hp_maximum(identity, x) : stat_maximum(identity, x)
maximum(x; threads = false) = Base.maximum(x)

minimum(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : threads ? hp_minimum(f, x) : stat_minimum(f, x)
minimum(f, x; threads = false) = Base.minimum(f, x)
minimum(x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : threads ? hp_minimum(identity, x) : stat_minimum(identity, x)
minimum(x; threads = false) = Base.minimum(x)
# TODO not optimised for simd - threads option is useless here / it is here because we have it for other types of data
# using Union{Missing, AbstractString} force to fall back to this definition for Vector{Missing} Julia >= 1.9
if VERSION >= v"1.9"
    _TASM_14329 = Union{Missing, AbstractString}
else
    _TASM_14329 = AbstractString
end
maximum(f, x::AbstractVector{Union{Missing, T}}; threads = false) where T <: _TASM_14329 = mapreduce(f, _stat_max_fun, x)
minimum(f, x::AbstractVector{Union{Missing, T}}; threads = false) where T <: _TASM_14329 = mapreduce(f, _stat_min_fun, x)
maximum(x::AbstractVector{Union{Missing, T}}; threads = false) where T <: _TASM_14329 = maximum(identity, x)
minimum(x::AbstractVector{Union{Missing, T}}; threads = false) where T <: _TASM_14329 = minimum(identity, x)

sum(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : threads ? hp_sum(f, x) : stat_sum(f, x)
sum(f, x; threads = false)=Base.sum(f, x)
sum(x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS} =isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) :  threads ? hp_sum(identity, x) : stat_sum(identity, x)
sum(x; threads = false) = Base.sum(x)

mean(f, x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_mean(f, x)
mean(x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_mean(x)
mean(f, x) = Statistics.mean(f, x)
mean(x) = Statistics.mean(x)

wsum(f, x::AbstractVector, w::AbstractVector) = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_wsum(f, x, w)
wsum(x::AbstractVector, w::AbstractVector) = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_wsum(identity, x, w)

wmean(f, x::AbstractVector, w::AbstractVector) = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_wmean(f, x, w)
wmean(x::AbstractVector, w::AbstractVector) = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_wmean(identity, x, w)

var(f, x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_var(f, x, dof)
var(f, x::AbstractArray{T,1}, dof = true) where T <: Union{INTEGERS, FLOATS}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_var(f, x, dof)
var(x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}=isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) :  stat_var(x, dof)
var(x::AbstractArray{T,1}, dof = true) where T <: Union{INTEGERS, FLOATS}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_var(x, dof)
var(x, dof = true) = Statistics.var(x, corrected = dof)

std(f, x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_std(f, x, dof)
std(x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_std(x, dof)
std(f, x::AbstractArray{T,1}, dof = true) where T <: Union{INTEGERS, FLOATS}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_std(f, x, dof)
std(x::AbstractArray{T,1}, dof = true) where T <: Union{INTEGERS, FLOATS}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_std(x, dof)

median(x::AbstractArray{Union{T,Missing},1}) where T = stat_median(x)
median!(x::AbstractArray{Union{T,Missing},1}) where T = stat_median!(x)
median(x) = Statistics.median(x)
median!(x) = Statistics.median!(x)



extrema(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : threads ? (hp_minimum(f, x), hp_maximum(f, x)) : (stat_minimum(f, x), stat_maximum(f, x))
extrema(f, x; threads = false) = Base.extrema(f, x)
extrema(x::AbstractArray{Union{Missing, T},1}; threads = false) where T  <: Union{INTEGERS, FLOATS, TimeType}= isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : threads ? (hp_minimum(identity, x), hp_maximum(identity, x)) : (stat_minimum(identity, x), stat_maximum(identity, x))
extrema(x; threads = false) = Base.extrema(x)

# when by is a function the following functions find argmax/min(by.(x))
argmax(x::AbstractArray{<:Union{Missing, T},1}; by = identity) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_findmax(by, x)[2]
argmax(x) = Base.argmax(x)
argmin(x::AbstractArray{<:Union{Missing, T},1}; by = identity) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_findmin(by, x)[2]
argmin(x) = Base.argmin(x)

findmax(f, x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_findmax(f, x)
findmax(f, x) = Base.findmax(f, x)
findmax(x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_findmax(x)
findmax(x) = Base.findmax(x)
findmin(f, x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_findmin(f, x)
findmin(f, x) = Base.findmin(f, x)
findmin(x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = isempty(x) ? throw(ArgumentError("empty arrays are not allowed")) : stat_findmin(x)
findmin(x) = Base.findmin(x)

function cumsum(x::AbstractArray{Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS}
    if missings == :ignore
        stat_cumsum_ignore(x)
    elseif missings == :skip
        stat_cumsum_skip(x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
cumsum(x; missings = :ignore) = Base.cumsum(x)
function cumsum!(outx::AbstractVector, x::AbstractArray{Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS}
    if missings == :ignore
        stat_cumsum!_ignore(outx, x)
    elseif missings == :skip
        stat_cumsum!_skip(outx, x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
cumsum!(x,y; missings = :ignore) = Base.cumsum!(x,y)
function cumprod(x::AbstractArray{Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS}
    if missings == :ignore
        stat_cumprod_ignore(x)
    elseif missings == :skip
        stat_cumprod_skip(x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
cumprod(x; missings = :ignore)=Base.cumprod(x)
function cumprod!(outx::AbstractVector, x::AbstractArray{Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS}
    if missings == :ignore
        stat_cumprod!_ignore(outx, x)
    elseif missings == :skip
        stat_cumprod!_skip(outx, x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
cumprod!(x, y; missings=:ignore) = Base.cumprod!(x,y)
function cummin(x::AbstractArray{<:Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS, TimeType}
    if missings == :ignore
        stat_cummin_ignore(x)
    elseif missings == :skip
        stat_cummin_skip(x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
function cummin!(outx, x::AbstractArray{<:Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS, TimeType}
    if missings == :ignore
        stat_cummin!_ignore(outx, x)
    elseif missings == :skip
        stat_cummin!_skip(outx, x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
function cummax(x::AbstractArray{<:Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS, TimeType}
    if missings == :ignore
        stat_cummax_ignore(x)
    elseif missings == :skip
        stat_cummax_skip(x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
function cummax!(outx, x::AbstractArray{<:Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS, TimeType}
    if missings == :ignore
        stat_cummax!_ignore(outx, x)
    elseif missings == :skip
        stat_cummax!_skip(outx, x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
