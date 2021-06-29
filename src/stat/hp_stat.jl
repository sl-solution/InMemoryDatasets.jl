function hp_maximum(f, x::AbstractVector{T}) where T
	n = length(x)
	nt = Threads.nthreads()
	cz = div(n, nt)
	cz == 0 && return stat_maximum(f, x)
	CT = Core.Compiler.return_type(f, (nonmissingtype(eltype(x)), ))
	if T >: Missing
		CT = Union{Missing, CT}
	end
	res = Vector{CT}(undef, nt)
	Threads.@threads for i in 1:nt
		i == nt ? hi = n : hi = i*cz
		res[i] = stat_maximum(f, x, lo = (i-1)*cz+1, hi = hi)
	end
	stat_maximum(identity, res)
end
hp_maximum(x::AbstractVector{T}) where T = hp_maximum(identity, x)


function hp_minimum(f, x::AbstractVector{T}) where T
	n = length(x)
	nt = Threads.nthreads()
	cz = div(n, nt)
	cz == 0 && return stat_minimum(f, x)
	CT = Core.Compiler.return_type(f, (nonmissingtype(eltype(x)), ))
	if T >: Missing
		CT = Union{Missing, CT}
	end
	res = Vector{CT}(undef, nt)
	Threads.@threads for i in 1:nt
		i == nt ? hi = n : hi = i*cz
		res[i] = stat_minimum(f, x, lo = (i-1)*cz+1, hi = hi)
	end
	stat_minimum(identity, res)
end
hp_minimum(x::AbstractVector{T}) where T = hp_minimum(identity, x)


function hp_sum(f, x::AbstractVector{T}) where T
	n = length(x)
	nt = Threads.nthreads()
	cz = div(n, nt)
	cz == 0 && return stat_sum(f, x)
	CT = Core.Compiler.return_type(f, (nonmissingtype(eltype(x)), ))
	if T >: Missing
		CT = Union{Missing, CT}
	end
	res = Vector{CT}(undef, nt)
	Threads.@threads for i in 1:nt
		i == nt ? hi = n : hi = i*cz
		res[i] = stat_sum(f, x, lo = (i-1)*cz+1, hi = hi)
	end
	stat_sum(identity, res)
end
hp_sum(x::AbstractVector{T}) where T = hp_sum(identity, x)
