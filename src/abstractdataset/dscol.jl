# internal function for easy accessing a view of a column
__!(col1::DatasetColumn) = col1.val
__!(col1::SubDatasetColumn) = view(col1.val, col1.selected_index)
const SubOrDSCol = Union{SubDatasetColumn,DatasetColumn}

# we treat DatasetColumn as a one-column data set. and we need to manage every thing ourselves
# we don't encourage people to use ds[!, 1] syntax, manipulating a column of a data set should happen in modify/!
# isequal also use for == , since we don't want missing be annoying
Base.parent(col1::DatasetColumn) = col1.ds

Base.length(col1::SubOrDSCol) = length(__!(col1))
Base.size(col1::SubOrDSCol) = size(__!(col1))
Base.size(col1::SubOrDSCol, i::Integer) = size(__!(col1), i)
Base.isapprox(col1::SubOrDSCol, col2::SubOrDSCol) = isapprox(__!(col1), __!(col2))
Base.first(col1::SubOrDSCol) = first(__!(col1))
Base.last(col1::SubOrDSCol) = last(__!(col1))
Base.eltype(col1::SubOrDSCol) = eltype(__!(col1))
Base.ndims(col1::SubOrDSCol) = ndims(__!(col1))
Base.ndims(::Type{<:SubDatasetColumn}) = 1
Base.isassigned(col1::SubOrDSCol, i) = isassigned(__!(col1), i)
Base.identity(col1::SubOrDSCol) = identity(__!(col1))
Base.similar(col1::SubOrDSCol, args...) = similar(__!(col1), args...)
Base.copy(col1::SubOrDSCol) = copy(__!(col1))
Base.pairs(col1::SubOrDSCol) = pairs(IndexLinear(), __!(col1))
Base.iterate(col1::SubOrDSCol, kwargs...) = iterate(__!(col1), kwargs...)
PooledArrays.PooledArray(col1::SubOrDSCol; arg...) = PooledArray(__!(col1); arg...)
Base.convert(T::Type{<:AbstractVector}, col1::SubOrDSCol) = convert(T, __!(col1))
DataAPI.refarray(col::SubOrDSCol) = DataAPI.refarray(__!(col))
DataAPI.refpool(col::SubOrDSCol) = DataAPI.refpool(__!(col))

Base.isequal(col1::SubOrDSCol, y::Any) = isequal(__!(col1), y)
Base.isequal(y::Any, col1::SubOrDSCol) = isequal(y, __!(col1))
Base.isequal(col1::SubOrDSCol, col2::SubOrDSCol) = isequal(__!(col1), __!(col2))

Base.:(==)(col1::SubOrDSCol, y::Any) = (==)(__!(col1), y)
Base.:(==)(y::Any, col1::SubOrDSCol) = (==)(y, __!(col1))
Base.:(==)(col1::SubOrDSCol, col2::SubOrDSCol) = isequal(__!(col1), __!(col2))
function Base.fill!(col::SubOrDSCol, i)
    fill!(__!(col), i)
    removeformat!(col.ds, col.col)
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    _modified(_attributes(parent(col.ds)))
    col
end

function Base.pop!(col::SubOrDSCol)
    res = pop!(__!(col))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    _modified(_attributes(parent(col.ds)))
    res
end
function Base.popfirst!(col::SubOrDSCol)
    res = popfirst!(__!(col))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    _modified(_attributes(parent(col.ds)))
    res
end
function Base.pushfirst!(col::SubOrDSCol, x)
    res = pushfirst!(__!(col), x)
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    _modified(_attributes(parent(col.ds)))
    res
end



Base.:(*)(col1::SubOrDSCol, x::Any) = *(__!(col1), x)
Base.:(+)(col1::SubOrDSCol, x::Any) = +(__!(col1), x)
Base.:(/)(col1::SubOrDSCol, x::Any) = /(__!(col1), x)
Base.:(-)(col1::SubOrDSCol, x::Any) = -(__!(col1), x)
Base.:(*)(x::Any, col1::SubOrDSCol) = *(x, __!(col1))
Base.:(+)(x::Any, col1::SubOrDSCol) = +(x, __!(col1))
Base.:(/)(x::Any, col1::SubOrDSCol) = /(x, __!(col1))
Base.:(-)(x::Any, col1::SubOrDSCol) = -(x, __!(col1))
Base.:(*)(col2::SubOrDSCol, col1::SubOrDSCol) = *(__!(col2), __!(col1))
Base.:(+)(col2::SubOrDSCol, col1::SubOrDSCol) = +(__!(col2), __!(col1))
Base.:(/)(col2::SubOrDSCol, col1::SubOrDSCol) = /(__!(col2), __!(col1))
Base.:(-)(col2::SubOrDSCol, col1::SubOrDSCol) = -(__!(col2), __!(col1))
function Base.convert(::Type{T}, x::T) where {T<:DatasetColumn}
    x
end
function Base.convert(::Type{T}, x::T) where {T<:SubDatasetColumn}
    x
end

# threads is on for SubOrDSCol since it naturally shouldn't be used for unfavourable situations
Base.maximum(f, col::SubOrDSCol; threads=true) = maximum(f, __!(col), threads=threads)
Base.maximum(col::SubOrDSCol; threads=true) = maximum(identity, __!(col), threads=threads)
Base.minimum(f, col::SubOrDSCol; threads=true) = minimum(f, __!(col), threads=threads)
Base.minimum(col::SubOrDSCol; threads=true) = minimum(identity, __!(col), threads=threads)
Base.sum(f, col::SubOrDSCol; threads=true) = sum(f, __!(col), threads=threads)
Base.sum(col::SubOrDSCol; threads=true) = sum(identity, __!(col), threads=threads)
mean(f, col::SubOrDSCol) = mean(f, __!(col))
mean(col::SubOrDSCol) = mean(identity, __!(col))
var(f, col::SubOrDSCol, dof=true) = var(f, __!(col), dof)
var(col::SubOrDSCol, dof=true) = var(identity, __!(col), dof)
std(f, col::SubOrDSCol, dof=true) = std(f, __!(col), dof)
std(col::SubOrDSCol, dof=true) = std(identity, __!(col), dof)
median(col::SubOrDSCol) = median(__!(col))
function median!(col::SubOrDSCol)
    median!(__!(col))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    _modified(_attributes(parent(col.ds)))
    col
end
Base.extrema(f, col::SubOrDSCol; threads=true) = extrema(f, __!(col), threads=threads)
Base.extrema(col::SubOrDSCol; threads=true) = extrema(identity, __!(col), threads=threads)
Base.argmax(col::SubOrDSCol; by=identity) = argmax(__!(col), by=by)
Base.argmin(col::SubOrDSCol; by=identity) = argmin(__!(col), by=by)
Base.findmax(f, col::SubOrDSCol) = findmax(f, __!(col))
Base.findmax(col::SubOrDSCol) = findmax(identity, __!(col))
Base.findmin(f, col::SubOrDSCol) = findmin(f, __!(col))
Base.findmin(col::SubOrDSCol) = findmin(identity, __!(col))
Base.cumsum(col::SubOrDSCol; missings=:ignore) = cumsum(__!(col), missings=missings)
Base.cumprod(col::SubOrDSCol; missings=:ignore) = cumprod(__!(col), missings=missings)
cummin(col::SubOrDSCol; missings=:ignore) = cummin(__!(col), missings=missings)
cummax(col::SubOrDSCol; missings=:ignore) = cummax(__!(col), missings=missings)

topk(col::SubOrDSCol, k; rev=false, lt = <, by = identity, threads = true) = topk(__!(col), k, rev=rev, lt = lt, by = by, threads = threads)
topkperm(col::SubOrDSCol, k; rev=false, lt = <, by = identity, threads = true) = topkperm(__!(col), k, rev=rev, lt = lt, by = by, threads = threads)
lag(col::SubOrDSCol; default=missing) = lag(__!(col), default=default)
lag(col::SubOrDSCol, k; default=missing) = lag(__!(col), k, default=default)
lead(col::SubOrDSCol; default=missing) = lead(__!(col), default=default)
lead(col::SubOrDSCol, k; default=missing) = lead(__!(col), k, default=default)

function lag!(col::SubOrDSCol; default=missing)
    lag!(__!(col), default=default)
    _modified(_attributes(parent(col.ds)))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    col
end
function lag!(col::SubOrDSCol, k; default=missing)
    lag!(__!(col), k, default=default)
    _modified(_attributes(parent(col.ds)))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    col
end
function lead!(col::SubOrDSCol; default=missing)
    lead!(__!(col), default=default)
    _modified(_attributes(parent(col.ds)))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    col
end
function lead!(col::SubOrDSCol, k; default=missing)
    lead!(__!(col), k, default=default)
    _modified(_attributes(parent(col.ds)))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    col
end



Base.Sort.defalg(col::SubOrDSCol) = Base.Sort.defalg(__!(col))
function Base.sort!(col::SubOrDSCol; alg::Base.Sort.Algorithm=Base.Sort.defalg(col), lt=isless, by=identity, rev::Bool=false, order::Base.Order.Ordering=Base.Order.Forward)
    sort!(__!(col), alg=alg, lt=lt, by=by, rev=rev, order=order)
    _modified(_attributes(parent(col.ds)))
    col.col ∈ index(parent(col.ds)).sortedcols && _reset_grouping_info!(parent(col.ds))
    col
end
function Base.sort(col::SubOrDSCol; alg::Base.Sort.Algorithm=Base.Sort.defalg(col), lt=isless, by=identity, rev::Bool=false, order::Base.Order.Ordering=Base.Order.Forward)
    sort(__!(col), alg=alg, lt=lt, by=by, rev=rev, order=order)
end

function Base.sortperm(col::SubOrDSCol; alg::Base.Sort.Algorithm=Base.Sort.DEFAULT_UNSTABLE, lt=isless, by=identity, rev::Bool=false, order::Base.Order.Ordering=Base.Order.Forward)
    sortperm(__!(col), alg=alg, lt=lt, by=by, rev=rev, order=order)
end
