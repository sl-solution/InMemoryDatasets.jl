Base.maximum(f, x::AbstractArray{Union{T,Missing},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = threads ? hp_maximum(f, x) : stat_maximum(f, x)
Base.maximum(x::AbstractArray{Union{T,Missing},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType}  = threads ? hp_maximum(identity, x) : stat_maximum(identity, x)

Base.minimum(f, x::AbstractArray{Union{T,Missing},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = threads ? hp_minimum(f, x) : stat_minimum(f, x)
Base.minimum(x::AbstractArray{Union{T,Missing},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType}= threads ? hp_minimum(identity, x) : stat_minimum(identity, x)

# not optimised for simd
Base.maximum(f, x::AbstractVector{Union{Missing, T}}) where T <: AbstractString = mapreduce(f, _stat_max_fun, x)
Base.minimum(f, x::AbstractVector{Union{Missing, T}}) where T <: AbstractString = mapreduce(f, _stat_min_fun, x)
Base.maximum(x::AbstractVector{Union{Missing, T}}) where T <: AbstractString = maximum(identity, x)
Base.minimum(x::AbstractVector{Union{Missing, T}}) where T <: AbstractString = minimum(identity, x)

Base.sum(f, x::AbstractArray{Union{T,Missing},1}; threads = false) where T <: Union{INTEGERS, FLOATS} = threads ? hp_sum(f, x) : stat_sum(f, x)
Base.sum(x::AbstractArray{Union{T,Missing},1}; threads = false) where T <: Union{INTEGERS, FLOATS} = threads ? hp_sum(identity, x) : stat_sum(identity, x)

Statistics.mean(f, x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS} = stat_mean(f, x)
Statistics.mean(x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS} = stat_mean(x)

wsum(f, x::AbstractVector, w::AbstractVector) = stat_wsum(f, x, w)
wsum(x::AbstractVector, w::AbstractVector) = stat_wsum(identity, x, w)

wmean(f, x::AbstractVector, w::AbstractVector) = stat_wmean(f, x, w)
wmean(x::AbstractVector, w::AbstractVector) = stat_wmean(identity, x, w)

Statistics.var(f, x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_var(f, x, dof)
Statistics.var(x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_var(x, dof)

Statistics.std(f, x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_std(f, x, dof)
Statistics.std(x::AbstractArray{Union{T,Missing},1}, dof = true) where T <: Union{INTEGERS, FLOATS}= stat_std(x, dof)

Statistics.median(x::AbstractArray{Union{T,Missing},1}) where T = stat_median(x)
Statistics.median!(x::AbstractArray{Union{T,Missing},1}) where T = stat_median!(x)



Base.extrema(f, x::AbstractArray{Union{T,Missing},1}; threads = false) where T <: Union{INTEGERS, FLOATS, TimeType} = threads ? (hp_minimum(f, x), hp_maximum(f, x)) : (stat_minimum(f, x), stat_maximum(f, x))
Base.extrema(x::AbstractArray{Union{T,Missing},1}; threads = false) where T  <: Union{INTEGERS, FLOATS, TimeType}= threads ? (hp_minimum(identity, x), hp_maximum(identity, x)) : (stat_minimum(identity, x), stat_maximum(identity, x))

Base.argmax(f, x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_argmax(f, x)
Base.argmax(x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_argmax(x)
Base.argmin(f, x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_argmin(f, x)
Base.argmin(x::AbstractArray{Union{T,Missing},1}) where T <: Union{INTEGERS, FLOATS, TimeType, AbstractString} = stat_argmin(x)
