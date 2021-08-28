# modified return_type to suit for our purpose
function return_type(f::Function, x::AbstractVector)
    CT = nonmissingtype(eltype(x))
    T = Core.Compiler.return_type(f, (Vector{CT}, ))
    # workaround for SubArray type
    if T <: SubArray
        return Core.Compiler.return_type(f, (typeof(x), ))
    end
    if T <: AbstractVector
        T = AbstractVector{Union{Missing, eltype(T)}}
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

_tmp_hash_fun(x, y) = hash(x, y)
_tmp_hash_fun(x::Characters, y) = hash(x.data, y)

function _create_dictionary!(prev_groups, groups, gslots, rhashes, f, v, prev_max_group)
    Threads.@threads for i in 1:length(v)
        @inbounds rhashes[i] = _tmp_hash_fun(f(v[i]), prev_groups[i])
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
    if ngroups == n
        flag = false
        return flag, ngroups
    end
    Threads.@threads for i in 1:length(rhashes)
        @inbounds prev_groups[i] = groups[i]
    end
    # copy!(prev_groups, rhashes)
    return flag, ngroups
end

function _gather_groups(ds, cols, ::Val{T}; mapformats = false) where T
    colidx = index(ds)[cols]
    _max_level = nrow(ds)
    prev_max_group = UInt(1)
    prev_groups = zeros(UInt, nrow(ds))
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


function _grouper_for_int_pool!(prev_group, groups, current_ngroups, y, f, minval, rangeval)
    ngroups = current_ngroups * rangeval
    flag = true

    # seen = falses(rangeval, current_ngroups)
    # TODO is it safe to thread it?
    seen = fill(false, rangeval, current_ngroups)
    Threads.@threads for i in 1:length(prev_group)
        seen[(f(y[i]) - minval + 1), prev_group[i]] = true
    end
    if sum(seen) < ngroups
        oldngroups = ngroups
        remap = zeros(Int, rangeval, current_ngroups)
        ngroups = 0
        @inbounds for i in eachindex(seen)
            ngroups += seen[i]
            remap[i] = ngroups
        end
        @inbounds Threads.@threads for i in eachindex(prev_group)
            gix = (f(y[i]) - minval + 1) + (prev_group[i] - 1)*rangeval
            prev_group[i] = remap[gix]
        end
    else
        @inbounds Threads.@threads for i in eachindex(prev_group)
            gix = (f(y[i]) - minval + 1) + (prev_group[i] - 1)*rangeval
            prev_group[i] = gix
        end
    end
    if ngroups == length(prev_group)
        flag = false
    end
    Threads.@threads for i in 1:length(groups)
        @inbounds groups[i] = prev_group[i]
    end
    return flag, ngroups
end

# ds assumes is grouped based on cols and groups are gathered togther
function  _find_starts_of_groups(ds, cols::Vector, ::Val{T}; mapformats = true) where T
    colsidx = index(ds)[cols]

    ranges = Vector{T}(undef, nrow(ds))
    ranges_cpy = copy(ranges)
    ranges[1] = 1
    ranges_cpy[1] = 1
    last_valid_index = 1

    for j in 1:length(colsidx)
        if mapformats
            _f = getformat(ds, colsidx[j])
        else
            _f = identity
        end
        last_valid_index = _find_starts_of_groups!(_columns(ds)[colsidx[j]], _f , ranges, ranges_cpy, last_valid_index)
    end
    return colsidx, ranges, last_valid_index
end

_find_starts_of_groups(ds, col::ColumnIndex, ::Val{T}; mapformats = true) where T = _find_starts_of_groups(ds, [col], Val(T), mapformats = mapformats)
_find_starts_of_groups(ds, cols::UnitRange, ::Val{T}; mapformats = true) where T = _find_starts_of_groups(ds, collect(cols), Val(T), mapformats = mapformats)


function _find_starts_of_groups!(x, format, ranges, ranges_cpy, last_valid_index)
    cnt = 1
    @inbounds for j in 1:last_valid_index
        lo = ranges_cpy[j]
        j == last_valid_index ? hi = length(x) : hi = ranges_cpy[j + 1] - 1
        ranges[cnt] = lo
        cnt += 1
        @inbounds for i in lo:(hi - 1)
            if !isequal(format(x[i]), format(x[i+1]))
                ranges[cnt] = i + 1
                cnt += 1
            end
        end
    end
    @inbounds for j in 1:(cnt - 1)
        ranges_cpy[j] = ranges[j]
    end
    return cnt - 1
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

# Compute chunks of indices, each with at least `basesize` entries
# This method ensures balanced sizes by avoiding a small last chunk
function split_indices(len::Integer, basesize::Integer)
    len′ = Int64(len) # Avoid overflow on 32-bit machines
    @assert len′ > 0
    @assert basesize > 0
    np = Int64(max(1, len ÷ basesize))
    return split_to_chunks(len′, np)
end

function split_to_chunks(len::Integer, np::Integer)
    len′ = Int64(len) # Avoid overflow on 32-bit machines
    np′ = Int64(np)
    @assert len′ > 0
    @assert 0 < np′ <= len′
    return (Int(1 + ((i - 1) * len′) ÷ np):Int((i * len′) ÷ np) for i in 1:np)
end

if VERSION >= v"1.4"
    function _spawn_for_chunks_helper(iter, lbody, basesize)
        lidx = iter.args[1]
        range = iter.args[2]
        quote
            let x = $(esc(range)), basesize = $(esc(basesize))
                @assert firstindex(x) == 1

                nt = Threads.nthreads()
                len = length(x)
                if nt > 1 && len > basesize
                    tasks = [Threads.@spawn begin
                                 for i in p
                                     local $(esc(lidx)) = @inbounds x[i]
                                     $(esc(lbody))
                                 end
                             end
                             for p in split_indices(len, basesize)]
                    foreach(wait, tasks)
                else
                    for i in eachindex(x)
                        local $(esc(lidx)) = @inbounds x[i]
                        $(esc(lbody))
                    end
                end
            end
            nothing
        end
    end
else
    function _spawn_for_chunks_helper(iter, lbody, basesize)
        lidx = iter.args[1]
        range = iter.args[2]
        quote
            let x = $(esc(range))
                for i in eachindex(x)
                    local $(esc(lidx)) = @inbounds x[i]
                    $(esc(lbody))
                end
            end
            nothing
        end
    end
end

"""
    @spawn_for_chunks basesize for i in range ... end

Parallelize a `for` loop by spawning separate tasks
iterating each over a chunk of at least `basesize` elements
in `range`.

A number of task higher than `Threads.nthreads()` may be spawned,
since that can allow for a more efficient load balancing in case
some threads are busy (nested parallelism).
"""
macro spawn_for_chunks(basesize, ex)
    if !(isa(ex, Expr) && ex.head === :for)
        throw(ArgumentError("@spawn_for_chunks requires a `for` loop expression"))
    end
    if !(ex.args[1] isa Expr && ex.args[1].head === :(=))
        throw(ArgumentError("nested outer loops are not currently supported by @spawn_for_chunks"))
    end
    return _spawn_for_chunks_helper(ex.args[1], ex.args[2], basesize)
end

function _nt_like_hash(v, h::UInt)
    length(v) == 0 && return hash(NamedTuple(), h)

    h = hash((), h)
    for i in length(v):-1:1
        h = hash(v[i], h)
    end

    return xor(objectid(Tuple(propertynames(v))), h)
end

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
