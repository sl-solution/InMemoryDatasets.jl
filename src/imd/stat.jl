
function maximum(f, x::Vector{T}) where T
	n = length(x)
	nt = Threads.nthreads()
	n < 10*nt && return mapreduce(identity, max, x)
	cz = div(n, nt)
	res = Vector{T}(undef, nt)
	Threads.@threads for i in 1:nt
		i == nt ? hi = n : hi = i*cz
		res[i] = Base.mapreduce_impl(f, max, x, (i-1)*cz+1, hi)
	end
	mapreduce(identity, max, res)
end

function fromhere()
	println("from imd")
end