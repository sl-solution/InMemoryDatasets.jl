maximum(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = threads ? hp_maximum(f, x) : stat_maximum(f, x)
maximum(f, x) = Base.maximum(f, x)
maximum(x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType}  = threads ? hp_maximum(identity, x) : stat_maximum(identity, x)
maximum(x) = Base.maximum(x)

minimum(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = threads ? hp_minimum(f, x) : stat_minimum(f, x)
minimum(f, x) = Base.minimum(f, x)
minimum(x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType}= threads ? hp_minimum(identity, x) : stat_minimum(identity, x)
minimum(x) = Base.minimum(x)
# TODO not optimised for simd - threads option is useless here / it is here because we have it for other types of data
maximum(f, x::AbstractVector{Union{Missing, T}}; threads = false) where T <: AbstractString = mapreduce(f, _stat_max_fun, x)
minimum(f, x::AbstractVector{Union{Missing, T}}; threads = false) where T <: AbstractString = mapreduce(f, _stat_min_fun, x)
maximum(x::AbstractVector{Union{Missing, T}}; threads = false) where T <: AbstractString = maximum(identity, x)
minimum(x::AbstractVector{Union{Missing, T}}; threads = false) where T <: AbstractString = minimum(identity, x)

sum(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS} = threads ? hp_sum(f, x) : stat_sum(f, x)
sum(f, x)=Base.sum(f, x)
sum(x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS} = threads ? hp_sum(identity, x) : stat_sum(identity, x)
sum(x) = Base.sum(x)

Statistics.mean(f, x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS} = stat_mean(f, x)
Statistics.mean(x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS} = stat_mean(x)

wsum(f, x::AbstractVector, w::AbstractVector) = stat_wsum(f, x, w)
wsum(x::AbstractVector, w::AbstractVector) = stat_wsum(identity, x, w)

wmean(f, x::AbstractVector, w::AbstractVector) = stat_wmean(f, x, w)
wmean(x::AbstractVector, w::AbstractVector) = stat_wmean(identity, x, w)

Statistics.var(f, x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_var(f, x, dof)
Statistics.var(x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_var(x, dof)

std(f, x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_std(f, x, dof)
std(x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_std(x, dof)

Statistics.median(x::AbstractArray{Union{T,Missing},1}) where T = stat_median(x)
Statistics.median!(x::AbstractArray{Union{T,Missing},1}) where T = stat_median!(x)



extrema(f, x::AbstractArray{Union{Missing, T},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = threads ? (hp_minimum(f, x), hp_maximum(f, x)) : (stat_minimum(f, x), stat_maximum(f, x))
extrema(f, x) = Base.extrema(f, x)
extrema(x::AbstractArray{Union{Missing, T},1}; threads = false) where T  <: Union{INTEGERS, FLOATS, TimeType}= threads ? (hp_minimum(identity, x), hp_maximum(identity, x)) : (stat_minimum(identity, x), stat_maximum(identity, x))
extrema(x) = Base.extrema(x)

# when by is a function the following functions find argmax/min(by.(x))
argmax(x::AbstractArray{Union{Missing, T},1}; by = identity) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_findmax(by, x)[2]
argmax(x) = Base.argmax(x)
argmin(x::AbstractArray{Union{Missing, T},1}; by = identity) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_findmin(by, x)[2]
argmin(x) = Base.argmin(x)

findmax(f, x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_findmax(f, x)
findmax(f, x) = Base.findmax(f, x)
findmax(x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_findmax(x)
findmax(x) = Base.findmax(x)
findmin(f, x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_findmin(f, x)
findmin(f, x) = Base.findmin(f, x)
findmin(x::AbstractArray{Union{Missing, T},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_findmin(x)
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
cumsum(x) = Base.cumsum(x)
function cumsum!(outx::AbstractVector, x::AbstractArray{Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS}
    if missings == :ignore
        stat_cumsum!_ignore(outx, x)
    elseif missings == :skip
        stat_cumsum!_skip(outx, x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
cumsum!(x,y) = Base.cumsum!(x,y)
function cumprod(x::AbstractArray{Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS}
    if missings == :ignore
        stat_cumprod_ignore(x)
    elseif missings == :skip
        stat_cumprod_skip(x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
cumprod(x)=Base.cumprod(x)
function cumprod!(outx::AbstractVector, x::AbstractArray{Union{Missing, T},1}; missings = :ignore) where T <: Union{INTEGERS, FLOATS}
    if missings == :ignore
        stat_cumprod!_ignore(outx, x)
    elseif missings == :skip
        stat_cumprod!_skip(outx, x)
    else
        throw(ArgumentError("`missings` must be either `:ignore` or `:skip`"))
    end
end
cumprod!(x,y) = Base.cumprod!(x,y)
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
