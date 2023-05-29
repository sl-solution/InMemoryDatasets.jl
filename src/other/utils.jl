const INTEGERS = Union{Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Bool}
const FLOATS = Union{Float16, Float32, Float64}

function our_nonmissingtype(x)
    T = nonmissingtype(x)
    if T === Union{}
        Missing
    else
        T
    end
end


# work around slow allocation of type union in julia
function _our_vect_alloc(T, len)
	if len > 0
		res = DataAPI.defaultarray(T, 1)(undef, 1)
		resize!(res, len)
	else
		DataAPI.defaultarray(T, 1)(undef, len)
	end
end

_missings(::Type{T}, len) where {T} = fill!(_our_vect_alloc(Union{T, Missing}, len), missing)



#macro for using Threaded for if needed
macro _threadsfor(threads, exp)
	esc(:(
		if $threads
			Threads.@threads $exp
		else
			$exp
		end
	))
end


# modified return_type to suit for our purpose
function return_type(f::Function, x)
	eltype(x) == Missing && return Missing
    
    if eltype(x) <: AbstractVector
        return return_type_tuple(f, x)
    end
    CT = our_nonmissingtype(eltype(x))
    T = Core.Compiler.return_type(f, Tuple{Vector{CT}})
    # workaround for SubArray type
    if T <: SubArray
        return Core.Compiler.return_type(f, Tuple{typeof(x)})
    elseif T <: AbstractVector
        T = AbstractVector{Union{Missing, eltype(T)}}
    elseif T <: Tuple
        T = Union{Missing, Core.Compiler.return_type(f, Tuple{Vector{eltype(x)}})}
    else
        T = Union{Missing, T}
    end
    T
end

function return_type_tuple(f::Function, x)
    CT = ntuple(i -> our_nonmissingtype(eltype(x[i])), length(x))
    T = Core.Compiler.return_type(f, Tuple{ntuple(i->Vector{CT[i]}, length(x))...})
    # workaround for SubArray type
    if T <: SubArray
        return Core.Compiler.return_type(f, Tuple{(typeof.(x))...})
    elseif T <: AbstractVector
        T = AbstractVector{Union{Missing, eltype(T)}}
    elseif T <: Tuple
        T = Union{Missing, Core.Compiler.return_type(f, Tuple{ntuple(i->Vector{eltype(x[i])}, length(x))...})}
    else
        T = Union{Missing, T}
    end
    T
end


Missings.allowmissing(a::PooledArray) = convert(PooledArray{Union{Missing, eltype(a)}}, a)

function allocatecol(x::AbstractVector, len; addmissing = true)
    if addmissing
        @assert len > 0 "cannot allocate a column with length zero"
    end
    if DataAPI.refpool(x) !== nothing
        if x isa PooledArray
            _res = PooledArray(PooledArrays.RefArray(x.refs[1:1]), DataAPI.invrefpool(x), DataAPI.refpool(x), PooledArrays.refcount(x))
        else
            # TODO not optimised for Categorical arrays
            _res = copy(x)
        end
        resize!(_res, len)
        if addmissing
            _res = allowmissing(_res)
            _res[1] = missing
        end
    else
        _res = _our_vect_alloc(Union{Missing, eltype(x)}, len)
    end
    return _res
end

function allocatecol(T, len)
    _our_vect_alloc(Union{Missing, T}, len)
end

function our_cumsum!(x)
    @inbounds for i in 2:length(x)
        x[i] += x[i-1]
    end
    x
end


function _generate_inverted_dict_pool(x)
    invp = DataAPI.invrefpool(x)
    if invp isa Dict
        return Dict{valtype(invp), Union{Missing, keytype(invp)}}(values(invp) .=> keys(invp))
    elseif invp.invpool isa Dict
        a = Dict{valtype(invp.invpool), Union{Missing, keytype(invp.invpool)}}(values(invp.invpool) .=> keys(invp.invpool))
        push!(a, 0 => missing)
    else
        throw(ArgumentError("$(typeof(x)) is not supported, used PooledArray or Categorical Array"))
    end
end


function _hp_map_a_function!(fv, f, v)
    Threads.@threads for i in 1:length(v)
        fv[i] = f(v[i])
    end
end

function _hp_map!_a_function!(x, f)
    Threads.@threads for i in 1:length(x)
        x[i] = f(x[i])
    end
end


function _first_nonmiss(x)
    for i in 1:length(x)
        res = x[i]
        !ismissing(res) && return res
    end
    res
end


# define a structure for gathered data
mutable struct START_END
	start::Bool
	sz::Int
	where
end

function _f_barrier_give_end!(y, sz)
	for i in 1:length(y)-1
		y[i] = y[i+1] - 1
	end
	y[end] = sz
end
function _f_barrier_give_start!(y)
	for i in length(y):-1:2
		y[i] = y[i-1] + 1
	end
	y[1] = 1
end

function Base.reverse!(x::START_END)
	if x.start
		_f_barrier_give_end!(x.where, x.sz)
		x.start = false
		return x
	else
		_f_barrier_give_start!(x.where)
		x.start = true
		return x
	end
end

struct GIVENRANGE
    idx
    starts
	starts_loc
	lastvalid
end

function _sortitout!(res, starts, x)
    fill!(starts, 0)
    starts[1] = 1
    for i in 1:length(x)
        starts[x[i] + 1] += 1
    end
	starts_normalised = map(>(0), starts)
    our_cumsum!(starts)
    for i in 1:length(x)
        label = x[i]
        res[starts[label]] = i
        starts[label] += 1
    end
	starts .-= 1
	reverse!(START_END(false, length(x), starts))
	return starts_normalised[2:end]
end

function _divide_for_fast_join_barrier!(res, starts, x, f, ::Val{T}; threads = true) where T
    nc = length(starts) - 1
    _hashed = Vector{T}(undef, length(x))
    @_threadsfor threads for i in 1:length(x)
        _hashed[i] = hash(f(x[i])) % nc + 1
    end
    starts_normalised = _sortitout!(res, starts, _hashed)
    return starts_normalised
end

function _remove_unwantedstarts!(starts, sz)
    curloc=2
    i = 1
    while true
        if starts[curloc] == starts[i]
            curloc += 1
        else
            i += 1
            starts[i] = starts[curloc]
        end
        starts[i]>sz && break
    end
    return resize!(starts, i-1)
end


function _divide_for_fast_join(x, f, chunk; threads = true) # chunk = 2^10 then data are divided to 1024 pieces
    T = length(x) < typemax(Int32) ? Int32 : Int64
    res = Vector{T}(undef, length(x))
    starts = Vector{T}(undef, chunk + 1)
    starts_loc = _divide_for_fast_join_barrier!(res, starts, x, f, chunk < typemax(UInt8) ? Val(UInt8) : chunk < typemax(UInt16) ? Val(UInt16) : Val(UInt32); threads = threads)
	starts = _remove_unwantedstarts!(starts, length(x))
	GIVENRANGE(res, starts, starts_loc, length(starts))
end


function _calculate_ends(groups, ngroups, ::Val{T}) where T
    where = zeros(T, ngroups)
    @inbounds for i = 1:length(groups)
        where[groups[i]] += 1
    end
    START_END(false, length(groups), our_cumsum!(where))
end


# From DataFrames.jl

function do_call(f::Base.Callable, incols::NTuple{2, AbstractVector}, r)
    return f(view(incols[1], r), view(incols[2], r))
end

function do_call(f::Base.Callable, incols::NTuple{3, AbstractVector}, r)
    return f(view(incols[1], r), view(incols[2], r),  view(incols[3], r))
end

function do_call(f::Base.Callable, incols::NTuple{4, AbstractVector}, r)
    return f(view(incols[1], r), view(incols[2], r),  view(incols[3], r), view(incols[4], r))
end

function do_call(f::Base.Callable, incols::Tuple, r)
    return f(map(c -> view(c, r), incols)...)
end

# Date & Time should be treated as integer
_date_value(::Missing) = missing
_date_value(x::TimeType) = Dates.value(x)::Int
_date_value(x::Period) = Dates.value(x)::Int
_date_value(x) = x


function _create_dictionary_unstable!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group, ::Val{T}; threads = true) where T
	_which_to_process = _find_groups_with_more_than_one_observation(prev_groups, prev_max_group)[1]
	offsets = findall(_which_to_process)
	_sum_of_which_to_process = sum(_which_to_process)
	@_threadsfor threads for i in 1:length(v)
		@inbounds if _which_to_process[prev_groups[i]]
			rhashes[i] = hash(f(v[i]), hash(prev_groups[i]))
		end
	end

    n = length(v)
    # sz = 2 ^ ceil(Int, log2(n)+1)
    sz = length(gslots)
    # fill!(gslots, 0)
    @_threadsfor threads for i in 1:sz
        @inbounds gslots[i] = 0
    end
    szm1 = sz - 1
    ngroups = prev_max_group - _sum_of_which_to_process
    flag = true
	@inbounds for i in eachindex(rhashes)
		if _which_to_process[prev_groups[i]]
        	slotix = rhashes[i] & szm1 + 1
        	gix = -1
        	probe = 0
        	while true
            	g_row = gslots[slotix]
            	if g_row == 0
                	gslots[slotix] = i
                	gix = ngroups += 1
                	break
            #check hash collision
            	elseif rhashes[i] == rhashes[g_row]
                	if isequal(prev_groups[i],prev_groups[g_row]) && isequal(f(v[i]), f(v[g_row]))
                    	gix = groups[g_row]
                    	break
                	end
            	end
            	slotix = slotix & szm1 + 1
            	probe += 1
            	@assert probe < sz
        	end
        	groups[i] = gix
    	end
	end

	@_threadsfor threads for i in 1:length(rhashes)
		@inbounds if !_which_to_process[prev_groups[i]]
			pos = searchsortedlast(offsets, prev_groups[i])
			prev_groups[i] -= pos
		else
			@inbounds _which_to_process[prev_groups[i]] ? prev_groups[i] = groups[i] : nothing
		end
    end
	if threads
		ngroups = hp_maximum(prev_groups)
	else
		ngroups = stat_maximum(prev_groups)
	end

    if ngroups == n
        flag = false
        return flag, ngroups
    end

    # copy!(prev_groups, rhashes)
    return flag, ngroups
end

function _create_dictionary!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group, stable, ::Val{T}; threads = true) where T
	if !stable
		return _create_dictionary_unstable!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group, Val(T); threads = threads)
	end
    @_threadsfor threads for i in 1:length(v)
        @inbounds rhashes[i] = hash(f(v[i]), hash(prev_groups[i])) #hash(prev_groups[i]) is used to prevent reduce probe, the question is: is it working?
    end
    n = length(v)
    # sz = 2 ^ ceil(Int, log2(n)+1)
    sz = length(gslots)
    # fill!(gslots, 0)
    @_threadsfor threads for i in 1:sz
        @inbounds gslots[i] = 0
    end
    szm1 = sz - 1
    ngroups = 0
    flag = true
    @inbounds for i in eachindex(rhashes)
        slotix = rhashes[i] & szm1 + 1
        gix = -1
        probe = 0
        while true
            g_row = gslots[slotix]
            if g_row == 0
                gslots[slotix] = i
                gix = ngroups += 1
                break
            #check hash collision
            elseif rhashes[i] == rhashes[g_row]
                if isequal(prev_groups[i],prev_groups[g_row]) && isequal(f(v[i]), f(v[g_row]))
                    gix = groups[g_row]
                    break
                end
            end
            slotix = slotix & szm1 + 1
            probe += 1
            @assert probe < sz
        end
        groups[i] = gix
    end
    @_threadsfor threads for i in 1:length(rhashes)
        @inbounds prev_groups[i] = groups[i]
    end
    if ngroups == n
        flag = false
        return flag, ngroups
    end

    # copy!(prev_groups, rhashes)
    return flag, ngroups
end

function _create_dictionary_int_fast!(prev_groups, f, v, prev_max_group, minval, rangelen, ::Val{T}) where T
    offset = 1 - minval
    n = length(v)
    ngroups = 0
    flag = true

    remap = zeros(T, prev_max_group, rangelen + 1)

    @inbounds for i in 1:length(v)
        slotix = f(v[i]) + offset
        if ismissing(slotix)
            slotix = rangelen + 1
        end
		prv_grp = prev_groups[i]
        if remap[prv_grp, slotix] == 0
            ngroups += 1
            remap[prv_grp, slotix] = ngroups
            prev_groups[i] = remap[prv_grp, slotix]
        else
            prev_groups[i] = remap[prv_grp, slotix]
        end
    end
    if ngroups == n
        flag = false
    end

    return flag, ngroups
end


function _gather_groups(ds, cols, ::Val{T}; mapformats = false, stable = true, threads = true) where T
    colidx = index(ds)[cols]
    _max_level = nrow(ds)


	if nrow(ds) > 2^23 && !stable && 5<length(colidx)<16 
		if !mapformats || all(==(identity), getformat.(Ref(ds), colidx))
			return _gather_groups_hugeds_multicols(ds, cols, Val(T); threads = threads)
		end
	end


    prev_max_group = UInt(1)
    prev_groups = ones(T, nrow(ds))
    groups = T[]
    # rhashes = Vector{UInt}(undef, nrow(ds))
    rhashes = UInt[]
    seen_nonint = false
    sz = max(1 + ((5 * _max_level) >> 2), 16)
    sz = 1 << (8 * sizeof(sz) - leading_zeros(sz - 1))
    @assert 4 * sz >= 5 * _max_level
    gslots = T[]

    for j in 1:length(colidx)
        _f = _date_value
        if mapformats
            _f = _date_value∘getformat(ds, colidx[j])
        end

        if DataAPI.refpool(_columns(ds)[colidx[j]]) !== nothing
            if _f == _date_value∘identity || !mapformats
                v = DataAPI.refarray(_columns(ds)[colidx[j]])
            else
                v = DataAPI.refarray(map(_f, _columns(ds)[colidx[j]]))
            end
            _f = identity
        else
            v = _columns(ds)[colidx[j]]
        end
        if our_nonmissingtype(Core.Compiler.return_type(_f, Tuple{our_nonmissingtype(eltype(v))})) <: Union{Missing, INTEGERS}
			if threads
				_minval = hp_minimum(_f, v)
			else
				_minval = stat_minimum(_f, v)
			end
            if ismissing(_minval)
                continue
            else
                minval::Integer = _minval
            end
			if threads
				maxval::Integer = hp_maximum(_f, v)
			else
				maxval = stat_maximum(_f, v)
			end
			rnglen = BigInt(maxval) - BigInt(minval) + 1
			o1 = false
			if rnglen < typemax(Int)
				o1 = true
				rangelen = Int(rnglen)
			end
			o2 = false
			if o1 && BigInt(prev_max_group)*rangelen < 2*length(v)
				o2 = true
			end
            if o1 && o2 && maxval < typemax(Int)
                flag, prev_max_group = _create_dictionary_int_fast!(prev_groups, _f, v, prev_max_group, minval, rangelen, Val(T))
            else
                if !seen_nonint
                    seen_nonint = true
                    resize!(rhashes, nrow(ds))
                    resize!(gslots, sz)
                    resize!(groups, nrow(ds))
                end
				prev_max_group > nrow(ds)/100 ? stablegather = stable : stablegather = true
                flag, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group, stablegather, Val(T); threads = threads)

            end
        else
            if !seen_nonint
                seen_nonint = true
                resize!(rhashes, nrow(ds))
                resize!(gslots, sz)
                resize!(groups, nrow(ds))
            end
			prev_max_group > nrow(ds)/100 ? stablegather = stable : stablegather = true
            flag, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group, stablegather, Val(T); threads = threads)
        end
        !flag && break
    end
    return prev_groups, gslots, prev_max_group
end

function _find_groups_with_more_than_one_observation(groups, ngroups)
    res = trues(length(groups))
    seen_groups = falses(ngroups)

    _nonunique_barrier!(res, groups, seen_groups)

	fill!(seen_groups, false)

    _find_groups_with_more_than_one_observation_barrier!(res, groups, seen_groups)
	seen_groups, res

end

function _find_groups_with_more_than_one_observation_barrier!(res, groups, seen_groups)
    @inbounds for i in 1:length(res)
        res[i] && !seen_groups[groups[i]] ? seen_groups[groups[i]] = true : nothing
    end
    nothing
end

### Special path for huge ds and multiple cols - trade off between compilation and performance
# table columns are passed as a tuple of vectors to ensure type specialization - From DataFrames.jl
isequal_row(cols::Tuple{AbstractVector}, r1::Int, r2::Int) =
    isequal(cols[1][r1], cols[1][r2])
isequal_row(cols::Tuple{Vararg{AbstractVector}}, r1::Int, r2::Int) =
    isequal(cols[1][r1], cols[1][r2]) && isequal_row(Base.tail(cols), r1, r2)

isequal_row(cols1::Tuple{AbstractVector}, r1::Int, cols2::Tuple{AbstractVector}, r2::Int) =
    isequal(cols1[1][r1], cols2[1][r2])
isequal_row(cols1::Tuple{Vararg{AbstractVector}}, r1::Int,
            cols2::Tuple{Vararg{AbstractVector}}, r2::Int) =
    isequal(cols1[1][r1], cols2[1][r2]) &&
        isequal_row(Base.tail(cols1), r1, Base.tail(cols2), r2)


_grabrefs(x) = DataAPI.refpool(x) == nothing ? x : DataAPI.refarray(x)
function _gather_groups_hugeds_multicols(ds, cols, ::Val{T}; threads::Bool = true) where T
	colidx = index(ds)[cols]
	rhashes = byrow(ds, hash, cols, threads = threads)
	colsvals = ntuple(i->_grabrefs(_columns(ds)[colidx[i]]), length(colidx))
    if threads
        rngs, sz = _gather_groups_hugeds_splitter(rhashes, Val(T))
        groups = Vector{T}(undef, length(rhashes))
        ngroups_all = _gather_groups_hugeds_collector(groups, rngs, sz, rhashes, colsvals, Val(T))
        ngroups = _gather_groups_hugeds_cleanup!(groups, ngroups_all, rngs, sz)
    else
        groups = Vector{T}(undef, length(rhashes))
        rng = 1:length(rhashes)
        ngroups = create_dict_hugeds_multicols!(groups, rng, colsvals, rhashes, Val(T))
    end
    groups, T[], ngroups
end

# TODO what happen if the values are not randomly grouped based on cols
function _gather_groups_hugeds_splitter(rhashes, ::Val{T}) where T
    nt = 997 # TODO this should be an argument, however, we must be careful that this value doesn't degrade actual dictionary creation in Subsequent steps
    sz = zeros(T, nt)
    # It is safe to record _ids - memory will be released and it does not add extra memory to the total amount (we later need to allocate groups)
    _id = Vector{Int16}(undef, length(rhashes))
    for i in eachindex(rhashes)
        _id[i] = (rhashes[i] % nt)+1
        sz[_id[i]] += 1
    end
    rngs = Vector{T}(undef, length(rhashes))
    prepend!(sz, T(0))
    our_cumsum!(sz)
    sz_cp = copy(sz)
   
    for i in eachindex(rhashes)
        idx=_id[i]
        sz_cp[idx] += 1
        rngs[sz_cp[idx]] = i
    end
    rngs, sz
end

function _gather_groups_hugeds_collector(groups, rngs, sz, rhashes, colsvals, ::Val{T}) where T
    ngroups = Vector{Int}(undef, length(sz)-1)
    Threads.@threads for i in 2:length(sz)
        hi = sz[i]
        lo = sz[i-1]+1
        _tmp = view(groups, view(rngs, lo:hi))
        ngroups[i-1] = create_dict_hugeds_multicols!(_tmp, view(rngs, lo:hi), colsvals, rhashes, Val(T))
    end
    ngroups
end

function _gather_groups_hugeds_cleanup!(groups, ngroups, rngs, sz)
    our_cumsum!(ngroups)
    Threads.@threads for i in 3:length(sz)
        hi=sz[i]
        lo=sz[i-1]+1
        for j in lo:hi
            groups[rngs[j]] += ngroups[i-2]
        end
    end
    return ngroups[end]
end

# groups is a list of integeres for which the dict is going to be created
# get index and set index should sometimes be adjusted based on rng
# make sure groups is a vector{T}
function create_dict_hugeds_multicols!(groups, rng, colvals, rhashes, ::Val{T}) where T
    isempty(rng) && return 0
	sz = max(1 + ((5 * length(groups)) >> 2), 16)
    sz = 1 << (8 * sizeof(sz) - leading_zeros(sz - 1))
    @assert 4 * sz >= 5 * length(groups)
    szm1 = sz-1
    gslots = zeros(T, sz)
    ngroups = 0
    @inbounds for i in eachindex(rng)
        # find the slot and group index for a row
        slotix = rhashes[rng[i]] & szm1 + 1
        gix = -1
        probe = 0
        while true
            g_row = gslots[slotix]
            if g_row == 0 # unoccupied slot, current row starts a new group
                gslots[slotix] = i
                gix = ngroups += 1
                break
            elseif rhashes[rng[i]] == rhashes[rng[g_row]] # occupied slot, check if miss or hit
                if isequal_row(colvals, Int(rng[i]), Int(rng[g_row])) # hit
                    gix = groups[g_row]
                    break
                end
            end
            slotix = slotix & szm1 + 1 # check the next slot
            probe += 1
            @assert probe < sz
        end
        # groups[i] has done its work we can modify it 
        groups[i] = gix
    end
    return ngroups
end


function _gather_groups_old_version(ds, cols, ::Val{T}; mapformats = false) where T
    colidx = index(ds)[cols]
    _max_level = nrow(ds)
    prev_max_group = UInt(1)
    prev_groups = ones(T, nrow(ds))
    groups = Vector{T}(undef, nrow(ds))
    rhashes = Vector{UInt}(undef, nrow(ds))
    sz = max(1 + ((5 * _max_level) >> 2), 16)
    sz = 1 << (8 * sizeof(sz) - leading_zeros(sz - 1))
    @assert 4 * sz >= 5 * _max_level
    gslots = Vector{T}(undef, sz)

    for j in 1:length(colidx)
        _f = identity
        if mapformats
            _f = getformat(ds, colidx[j])
        end

        if DataAPI.refpool(_columns(ds)[colidx[j]]) !== nothing
            if _f == identity
                v = DataAPI.refarray(_columns(ds)[colidx[j]])
            else
                v = DataAPI.refarray(map(_f, _columns(ds)[colidx[j]]))
            end
            _f = identity
        else
            v = _columns(ds)[colidx[j]]
        end
        flag, prev_max_group = InMemoryDatasets._create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group)

        !flag && break
    end
    return groups, gslots, prev_max_group
end

# ds assumes is grouped based on cols and groups are gathered togther
function _find_starts_of_groups(ds, cols::MultiColumnIndex, ::Val{T}; mapformats = true, threads = true) where T
    colsidx = index(ds)[cols]
    ranges = Vector{T}(undef, nrow(ds))
    inbits = zeros(Bool, nrow(ds))
    inbits[1] = true
    last_valid_index = 1
    for j in 1:length(colsidx)
        if mapformats
            _f = getformat(ds, colsidx[j])
        else
            _f = identity
        end
        _find_starts_of_groups!(_columns(ds)[colsidx[j]], _get_perms(ds; threads = threads), _f , inbits; threads = threads)
	all(inbits) && break
    end
    @inbounds for i in 1:length(inbits)
        if inbits[i]
            ranges[last_valid_index] = i
            last_valid_index += 1
        end
    end
    return collect(colsidx), ranges, (last_valid_index - 1)
end

_find_starts_of_groups(ds, col::ColumnIndex, ::Val{T}; mapformats = true, threads = true) where T = _find_starts_of_groups(ds, [col], Val(T), mapformats = mapformats, threads = threads)

function _find_starts_of_groups!(x, perm, f, inbits; threads = true)
    @_threadsfor threads for i in 2:length(inbits)
        @inbounds if !inbits[i]
		inbits[i] = !isequal(f(x[perm[i]]), f(x[perm[i-1]]))
	end
    end
end
# function _find_starts_of_groups!(x, perm, f, inbits, starts, ngroups)
# 	Threads.@threads for j in 1:ngroups
# 		i = starts[j]
# 		@inbounds inbits[i] = inbits[i]==1 ? 1 : !isequal(f(x[perm[i]]), f(x[perm[i-1]]))
# 	end
# end

function make_unique!(names::Vector{Symbol}, src::AbstractVector{Symbol};
                      makeunique::Bool=false)
    if length(names) != length(src)
        throw(DimensionMismatch("Length of src doesn't match length of names."))
    end
    seen = Set{Symbol}()
    dups = Int[]
	dups_dict = Dict{Symbol, Int}()
    for i in 1:length(names)
        name = src[i]
        if in(name, seen)
            push!(dups, i)
			if ismissing(get(dups_dict, src[i], missing))
				dups_dict[src[i]] = 1
			end
        else
            names[i] = src[i]
            push!(seen, name)
        end
    end

    if length(dups) > 0
        if !makeunique
            dupstr = join(string.(':', unique(src[dups])), ", ", " and ")
            msg = "Duplicate variable names: $dupstr. Pass makeunique=true " *
                  "to make them unique using a suffix automatically."
            throw(ArgumentError(msg))
        end
    end
    for i in dups
        nm = src[i]
		dup_info = get(dups_dict, src[i], missing)
		if ismissing(dup_info)
			throw(ErrorException("Something is wrong"))
		else
			k = dup_info
			cnt = 1
			while true
				newnm = Symbol("$(nm)_$(k)")
				if !in(newnm, seen)
	                names[i] = newnm
	                push!(seen, newnm)
					break
	            end
				k += 1
				cnt += 1
			end
			dups_dict[src[i]] += cnt
		end
    end

    return names
end

function make_unique(names::AbstractVector{Symbol}; makeunique::Bool=false)
    make_unique!(similar(names), names, makeunique=makeunique)
end

"""
    gennames(n::Integer)

Generate standardized names for columns of a DataFrame.
The first name will be `:x1`, the second `:x2`, etc.
"""
function gennames(n::Integer)
    res = Vector{Symbol}(undef, n)
    for i in 1:n
        res[i] = Symbol(@sprintf "x%d" i)
    end
    return res
end

function funname(f)
    local n
    try
        n = nameof(f)
    catch
        return :function
    end
    if String(n)[1] == '#'
        :function
    elseif String(n) == "Fix2"
        nameof(f.f)
    elseif String(n) == "Fix1"
        nameof(f.f)
    else
        n
    end
end

if isdefined(Base, :ComposedFunction) # Julia >= 1.6.0-DEV.85
    using Base: ComposedFunction
else
    using Compat: ComposedFunction
end

funname(c::ComposedFunction) = Symbol(funname(c.outer), :_, funname(c.inner))

_findall(B) = findall(B)

_findall(B::AbstractVector{Bool}) = findall(convert(BitVector, B))

# this function is needed as == does not allow for comparison between tuples and vectors
function _equal_names(r1, r2)
    n1 = _getnames(r1)
    n2 = _getnames(r2)
    length(n1) == length(n2) || return false
    for (a, b) in zip(n1, n2)
        a == b || return false
    end
    return true
end

# a structure for vcat two vectors without allocation
struct Cat2Vec{F1, F2, CT, T, S, A, B} <: AbstractVector{Union{T, S}}
	vec1::A
	vec2::B
	f1::F1
	f2::F2
	len1::Int
	len2::Int
	function Cat2Vec(x , y, f1::F1, f2::F2) where {F1, F2}
		if length(x) > length(y)
			if DataAPI.invrefpool(x) !== nothing
				if f1 != identity
					v = map(f1, x)
				else
					v = x
				end
				dict = DataAPI.invrefpool(v)
				# It is workaround for Categorical data
				if hasproperty(dict, :invpool)
					vtype = valtype(dict.invpool)
				else
					vtype = valtype(dict)
				end
				res = Vector{Union{Missing, vtype}}(undef, length(y))
				_rev_map_invrefpool!(res, dict, y, f2)
				new{typeof(identity), typeof(identity), Union{Missing, vtype}, Union{vtype, Missing}, Union{vtype, Missing}, typeof(res), typeof(res)}(DataAPI.refarray(v), res, identity, identity, length(x), length(y))
			elseif DataAPI.invrefpool(y) !== nothing
				if f2 != identity
					v = map(f2, y)
				else
					v = y
				end
				dict = DataAPI.invrefpool(v)
				if hasproperty(dict, :invpool)
					vtype = valtype(dict.invpool)
				else
					vtype = valtype(dict)
				end
				res =  Vector{Union{Missing, vtype}}(undef, length(x))
				_rev_map_invrefpool!(res, dict, x, f1)
				new{typeof(identity), typeof(identity), Union{Missing, vtype}, Union{vtype, Missing}, Union{vtype, Missing}, typeof(res), typeof(res)}(res, DataAPI.refarray(v), identity, identity, length(x), length(y))
			else
				new{F1, F2, promote_type(Core.Compiler.return_type(f1, Tuple{eltype(x)}), Core.Compiler.return_type(f2, Tuple{eltype(y)})), eltype(x), eltype(y), typeof(x), typeof(y)}(x, y, f1, f2, length(x), length(y))
			end
		else
			if DataAPI.invrefpool(y) !== nothing
				if f2 != identity
					v = map(f2, y)
				else
					v = y
				end
				dict = DataAPI.invrefpool(v)
				if hasproperty(dict, :invpool)
					vtype = valtype(dict.invpool)
				else
					vtype = valtype(dict)
				end
				res =  Vector{Union{Missing, vtype}}(undef, length(x))
				_rev_map_invrefpool!(res, dict, x, f1)
				new{typeof(identity), typeof(identity), Union{Missing, vtype}, Union{vtype, Missing}, Union{vtype, Missing}, typeof(res), typeof(res)}(res, DataAPI.refarray(v), identity, identity, length(x), length(y))
			elseif DataAPI.invrefpool(x) !== nothing
				if f1 != identity
					v = map(f1, x)
				else
					v = x
				end
				dict = DataAPI.invrefpool(v)
				if hasproperty(dict, :invpool)
					vtype = valtype(dict.invpool)
				else
					vtype = valtype(dict)
				end
				res = Vector{Union{Missing, vtype}}(undef, length(y))
				_rev_map_invrefpool!(res, dict, y, f2)
				new{typeof(identity), typeof(identity), Union{Missing ,vtype}, Union{vtype, Missing}, Union{vtype, Missing}, typeof(res), typeof(res)}(DataAPI.refarray(v), res, identity, identity, length(x), length(y))
			else
				new{F1, F2, promote_type(Core.Compiler.return_type(f1, Tuple{eltype(x)}), Core.Compiler.return_type(f2, Tuple{eltype(y)})), eltype(x), eltype(y), typeof(x), typeof(y)}(x, y, f1, f2, length(x), length(y))
			end
		end
	end

end

function _rev_map_invrefpool!(res, dict, y, f)
	Threads.@threads for i in 1:length(res)
		res[i] = get(dict, DataAPI.unwrap(f(y[i])), missing)

	end
end

function __getindex(v::Cat2Vec{F1, F2, CT, T,S, A, B}, i::Int, f1, f2 )::CT where {F1, F2, CT, T, S, A, B}
    if i <= v.len1
		f1(v.vec1[i]::T)
	else
		f2(v.vec2[i-v.len1]::S)
	end
end


function Base.getindex(v::Cat2Vec{F1, F2, CT, T, S, A, B}, i::Int)::CT where {F1, F2, CT, T, S, A, B}
	__getindex(v, i, v.f1, v.f2)
end

Base.IndexStyle(::Type{<:Cat2Vec}) = Base.IndexLinear()
Base.size(v::Cat2Vec) = (length(v),)
Base.length(v::Cat2Vec) = v.len1 + v.len2
Base.eltype(v::Cat2Vec{F1, F2, CT, T, S, A, B}) where {F1, F2, CT, T, S, A, B} = CT
