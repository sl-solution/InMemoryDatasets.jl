# modified return_type to suit for our purpose
function return_type(f::Function, x::AbstractVector)
    CT = nonmissingtype(eltype(x))
    if CT <: AbstractVector
        return return_type_tuple(f, x)
    end
    T = Core.Compiler.return_type(f, (Vector{CT}, ))
    # workaround for SubArray type
    if T <: SubArray
        return Core.Compiler.return_type(f, (typeof(x), ))
    elseif T <: AbstractVector
        T = AbstractVector{Union{Missing, eltype(T)}}
    elseif T <: Tuple
        T = Union{Missing, Core.Compiler.return_type(f, (Vector{eltype(x)}, ))}
    else
        T = Union{Missing, T}
    end
    T
end

function return_type_tuple(f::Function, x::AbstractVector)
    CT1 = nonmissingtype(eltype(x[1]))
    CT2 = nonmissingtype(eltype(x[2]))
    T = Core.Compiler.return_type(f, (Vector{CT1}, Vector{CT2}, ))
    # workaround for SubArray type
    if T <: SubArray
        return Core.Compiler.return_type(f, (typeof(x), ))
    elseif T <: AbstractVector
        T = AbstractVector{Union{Missing, eltype(T)}}
    elseif T <: Tuple
        T = Union{Missing, Core.Compiler.return_type(f, (Vector{eltype(x[1])}, Vector{eltype(x[2])}, ))}
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
        _res = Tables.allocatecolumn(Union{Missing, eltype(x)}, len)
    end
    return _res
end

function allocatecol(T, len)
    Tables.allocatecolumn(Union{Missing, T}, len)
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
    cumsum!(starts, starts)
    for i in 1:length(x)
        label = x[i]
        res[starts[label]] = i
        starts[label] += 1
    end
	starts .-= 1
	reverse!(START_END(false, length(x), starts))
	return starts_normalised[2:end]
end

function _divide_for_fast_join_barrier!(res, starts, x, f, ::Val{T}) where T
    nc = length(starts) - 1
    _hashed = Vector{T}(undef, length(x))
    Threads.@threads for i in 1:length(x)
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


function _divide_for_fast_join(x, f, chunk) # chunk = 2^10 then data are divided to 1024 pieces
    T = length(x) < typemax(Int32) ? Int32 : Int64
    res = Vector{T}(undef, length(x))
    starts = Vector{T}(undef, chunk + 1)
    starts_loc = _divide_for_fast_join_barrier!(res, starts, x, f, chunk < typemax(UInt8) ? Val(UInt8) : chunk < typemax(UInt16) ? Val(UInt16) : Val(UInt32))
	starts = _remove_unwantedstarts!(starts, length(x))
	GIVENRANGE(res, starts, starts_loc, length(starts))
end


function _calculate_ends(groups, ngroups, ::Val{T}) where T
    where = zeros(T, ngroups)
    @inbounds for i = 1:length(groups)
        where[groups[i]] += 1
    end
    START_END(false, length(groups), cumsum!(where, where))
end


# Date & Time should be treated as integer
_date_value(::Missing) = missing
_date_value(x::TimeType) = Dates.value(x)::Int
_date_value(x::Period) = Dates.value(x)::Int
_date_value(x) = x


function _create_dictionary_unstable!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group, ::Val{T}) where T
	_which_to_process, offsets = _find_groups_with_more_than_one_observation(prev_groups, prev_max_group, Val(T))
	_sum_of_which_to_process = sum(_which_to_process)
	Threads.@threads for i in 1:length(v)
		@inbounds if _which_to_process[prev_groups[i]]
			rhashes[i] = hash(f(v[i]), hash(prev_groups[i]))
		end
	end

    n = length(v)
    # sz = 2 ^ ceil(Int, log2(n)+1)
    sz = length(gslots)
    # fill!(gslots, 0)
    Threads.@threads for i in 1:sz
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

	Threads.@threads for i in 1:length(rhashes)
		@inbounds if !_which_to_process[prev_groups[i]]
			pos = searchsortedlast(offsets, prev_groups[i])
			prev_groups[i] -= pos
		else
			@inbounds _which_to_process[prev_groups[i]] ? prev_groups[i] = groups[i] : nothing
		end
    end
	ngroups = hp_maximum(prev_groups)

    if ngroups == n
        flag = false
        return flag, ngroups
    end

    # copy!(prev_groups, rhashes)
    return flag, ngroups
end

function _create_dictionary!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group, stable, ::Val{T}) where T
	if !stable
		return _create_dictionary_unstable!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group, Val(T))
	end
    Threads.@threads for i in 1:length(v)
        @inbounds rhashes[i] = hash(f(v[i]), hash(prev_groups[i])) #hash(prev_groups[i]) is used to prevent reduce probe, the question is: is it working?
    end
    n = length(v)
    # sz = 2 ^ ceil(Int, log2(n)+1)
    sz = length(gslots)
    # fill!(gslots, 0)
    Threads.@threads for i in 1:sz
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
    Threads.@threads for i in 1:length(rhashes)
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


function _gather_groups(ds, cols, ::Val{T}; mapformats = false, stable = true) where T
    colidx = index(ds)[cols]
    _max_level = nrow(ds)
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
        if nonmissingtype(Core.Compiler.return_type(_f, (nonmissingtype(eltype(v)),))) <: Union{Missing, Integer}
            _minval = hp_minimum(_f, v)
            if ismissing(_minval)
                continue
            else
                minval::Integer = _minval
            end
            maxval::Integer = hp_maximum(_f, v)
            (diff, o1) = sub_with_overflow(Int(maxval), Int(minval))
            (rangelen, o2) = add_with_overflow(diff, oneunit(diff))
            (outmult, o3) = mul_with_overflow(rangelen, Int(prev_max_group))
            if !o1 && !o2 && !o3 && maxval < typemax(Int) &&  prev_max_group*rangelen < 2*length(v)
                flag, prev_max_group = _create_dictionary_int_fast!(prev_groups, _f, v, prev_max_group, minval, rangelen, Val(T))
            else
                if !seen_nonint
                    seen_nonint = true
                    resize!(rhashes, nrow(ds))
                    resize!(gslots, sz)
                    resize!(groups, nrow(ds))
                end
				prev_max_group > nrow(ds)/100 ? stablegather = stable : stablegather = false
                flag, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group, stablegather, Val(T))

            end
        else
            if !seen_nonint
                seen_nonint = true
                resize!(rhashes, nrow(ds))
                resize!(gslots, sz)
                resize!(groups, nrow(ds))
            end
			prev_max_group > nrow(ds)/100 ? stablegather = stable : stablegather = false
            flag, prev_max_group = _create_dictionary!(prev_groups, groups, gslots, rhashes, _f, v, prev_max_group, stablegather, Val(T))
        end
        !flag && break
    end
    return prev_groups, gslots, prev_max_group
end

function _find_groups_with_more_than_one_observation(groups, ngroups, ::Val{T}) where T
    res = trues(length(groups))
    seen_groups = falses(ngroups)

    _nonunique_barrier!(res, groups, seen_groups)

	fill!(seen_groups, false)

    _find_groups_with_more_than_one_observation_barrier!(res, groups, seen_groups)
	seen_groups, findall(seen_groups)

end

function _find_groups_with_more_than_one_observation_barrier!(res, groups, seen_groups)
    @inbounds for i in 1:length(res)
        res[i] && !seen_groups[groups[i]] ? seen_groups[groups[i]] = true : nothing
    end
    nothing
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
function _find_starts_of_groups(ds, cols::Vector, ::Val{T}; mapformats = true) where T
    colsidx = index(ds)[cols]
    #sortedidx = _sortedcols(ds)
	#starts = _group_starts(ds)
	#ngroups = _ngroups(ds)

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
        if !(typeof(ds) <: SubDataset) && length(colsidx) <= length(_sortedcols(ds)) && colsidx == view(_sortedcols(ds), 1:length(colsidx))
		    _find_starts_of_groups!(_columns(ds)[colsidx[j]], _f , inbits, _group_starts(ds), _ngroups(ds))
        else
            _find_starts_of_groups!(_columns(ds)[colsidx[j]], _f , inbits)
        end
    end
    @inbounds for i in 1:length(inbits)
        if inbits[i] == true
            ranges[last_valid_index] = i
            last_valid_index += 1
        end
    end
    return colsidx, ranges, (last_valid_index - 1)
end

_find_starts_of_groups(ds, col::ColumnIndex, ::Val{T}; mapformats = true) where T = _find_starts_of_groups(ds, [col], Val(T), mapformats = mapformats)
_find_starts_of_groups(ds, cols::UnitRange, ::Val{T}; mapformats = true) where T = _find_starts_of_groups(ds, collect(cols), Val(T), mapformats = mapformats)

function _find_starts_of_groups!(x, f, inbits)
    Threads.@threads for j in 2:length(inbits)
        @inbounds inbits[j] = inbits[j]==true ? true : !isequal(f(x[j]), f(x[j-1]))
    end
end
function _find_starts_of_groups!(x, f, inbits, starts, ngroups)
	Threads.@threads for i in 1:ngroups
		@inbounds inbits[starts[i]] = inbits[starts[i]]==1 ? 1 : !isequal(f(x[starts[i]]), f(x[starts[i]-1]))
	end
end

function make_unique!(names::Vector{Symbol}, src::AbstractVector{Symbol};
                      makeunique::Bool=false)
    if length(names) != length(src)
        throw(DimensionMismatch("Length of src doesn't match length of names."))
    end
    seen = Set{Symbol}()
    dups = Int[]
    for i in 1:length(names)
        name = src[i]
        if in(name, seen)
            push!(dups, i)
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
        k = 1
        while true
            newnm = Symbol("$(nm)_$k")
            if !in(newnm, seen)
                names[i] = newnm
                push!(seen, newnm)
                break
            end
            k += 1
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
    n = nameof(f)
    String(n)[1] == '#' ? :function : n
end

if isdefined(Base, :ComposedFunction) # Julia >= 1.6.0-DEV.85
    using Base: ComposedFunction
else
    using Compat: ComposedFunction
end

funname(c::ComposedFunction) = Symbol(funname(c.outer), :_, funname(c.inner))

_findall(B) = findall(B)

function _findall(B::AbstractVector{Bool})
    @assert firstindex(B) == 1
    nnzB = count(B)

    # fast path returning range
    nnzB == 0 && return 1:0
    len = length(B)
    nnzB == len && return 1:len
    start::Int = findfirst(B)
    nnzB == 1 && return start:start
    start + nnzB - 1 == len && return start:len
    stop::Int = findnext(!, B, start + 1) - 1
    start + nnzB == stop + 1 && return start:stop

    # slow path returning Vector{Int}
    I = Vector{Int}(undef, nnzB)
    @inbounds for i in 1:stop - start + 1
        I[i] = start + i - 1
    end
    cnt = stop - start + 2
    @inbounds for i in stop+1:len
        if B[i]
            I[cnt] = i
            cnt += 1
        end
    end
    @assert cnt == nnzB + 1
    return I
end

@inline _blsr(x) = x & (x-1)

# findall returning a range when possible (all true indices are contiguous), and optimized for B::BitVector
# the main idea is taken from Base.findall(B::BitArray)
function _findall(B::BitVector)::Union{UnitRange{Int}, Vector{Int}}
    nnzB = count(B)
    nnzB == 0 && return 1:0
    nnzB == length(B) && return 1:length(B)
    local I
    Bc = B.chunks
    Bi = 1 # block index
    i1 = 1 # index of current block beginng in B
    i = 1  # index of the _next_ one in I
    c = Bc[1] # current block

    start = -1 # the begining of ones block
    stop = -1  # the end of ones block

    @inbounds while true # I not materialized
        if i > nnzB # all ones in B found
            Ir = start:start + i - 2
            @assert length(Ir) == nnzB
            return Ir
        end

        if c == 0
            if start != -1 && stop == -1
                stop = i1 - 1
            end
            while c == 0 # no need to return here as we returned above
                i1 += 64
                Bi += 1
                c = Bc[Bi]
            end
        end
        if c == ~UInt64(0)
            if stop != -1
                I = Vector{Int}(undef, nnzB)
                for j in 1:i-1
                    I[j] = start + j - 1
                end
                break
            end
            if start == -1
                start = i1
            end
            while c == ~UInt64(0)
                if Bi == length(Bc)
                    Ir = start:length(B)
                    @assert length(Ir) == nnzB
                    return Ir
                end

                i += 64
                i1 += 64
                Bi += 1
                c = Bc[Bi]
            end
        end
        if c != 0 # mixed ones and zeros in block
            tz = trailing_zeros(c)
            lz = leading_zeros(c)
            co = c >> tz == (one(UInt64) << (64 - lz - tz)) - 1 # block of countinous ones in c
            if stop != -1  # already found block of ones and zeros, just not materialized
                I = Vector{Int}(undef, nnzB)
                for j in 1:i-1
                    I[j] = start + j - 1
                end
                break
            elseif !co # not countinous ones
                I = Vector{Int}(undef, nnzB)
                if start != -1
                    for j in 1:i-1
                        I[j] = start + j - 1
                    end
                end
                break
            else # countinous block of ones
                if start != -1
                    if tz > 0 # like __1111__ or 111111__
                        I = Vector{Int}(undef, nnzB)
                        for j in 1:i-1
                            I[j] = start + j - 1
                        end
                        break
                    else # lz > 0, like __111111
                        stop = i1 + (64 - lz) - 1
                        i += 64 - lz

                        # return if last block
                        if Bi == length(Bc)
                            Ir = start:stop
                            @assert length(Ir) == nnzB
                            return Ir
                        end

                        i1 += 64
                        Bi += 1
                        c = Bc[Bi]
                    end
                else # start == -1
                    start = i1 + tz

                    if lz > 0 # like __111111 or like __1111__
                        stop = i1 + (64 - lz) - 1
                        i += stop - start + 1
                    else # like 111111__
                        i += 64 - tz
                    end

                    # return if last block
                    if Bi == length(Bc)
                        Ir = start:start + i - 2
                        @assert length(Ir) == nnzB
                        return Ir
                    end

                    i1 += 64
                    Bi += 1
                    c = Bc[Bi]
                end
            end
        end
    end
    @inbounds while true # I materialized, process like in Base.findall
        if i > nnzB # all ones in B found
            @assert nnzB == i - 1
            return I
        end

        while c == 0 # no need to return here as we returned above
            i1 += 64
            Bi += 1
            c = Bc[Bi]
        end

        while c == ~UInt64(0)
            for j in 0:64-1
                I[i + j] = i1 + j
            end
            i += 64
            if Bi == length(Bc)
                @assert nnzB == i - 1
                return I
            end
            i1 += 64
            Bi += 1
            c = Bc[Bi]
        end

        while c != 0
            tz = trailing_zeros(c)
            c = _blsr(c) # zeros last nonzero bit
            I[i] = i1 + tz
            i += 1
        end
    end
    @assert false "should not be reached"
end
