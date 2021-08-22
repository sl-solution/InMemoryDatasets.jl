# forked from FixedSizeStrings.jl

import Base: iterate, lastindex, getindex, sizeof, length, ncodeunits, codeunit, isvalid, read, write, setindex!, string, convert

struct Characters{N, M} <: AbstractString
    data::NTuple{N, M}
    function Characters{N, M}(v::Vector{UInt8}) where N where M
        new(NTuple{N, M}(v))
    end
    function Characters{N, M}(itr) where {N} where {M}
        isempty(itr) && return missing
        new(NTuple{N, M}(rpad(itr, N)))
    end
end

function Characters{N, M}(v::Vector{UInt8}, v2) where N where M

    @simd for i in 1:min(N, length(v))
        @inbounds v2[i] = v[i]
    end
    @simd for i in length(v)+1:N
        @inbounds v2[i] = 0x20
    end

    Characters{N, M}(v2)
end
function Characters{N, M}(v::Vector{UInt8}, v2, st, en) where N where M
    @simd for i in 1:min(N, en - st + 1)
        @inbounds v2[i] = v[i + st - 1]
    end
    @simd for i in en - st + 2:N
        @inbounds v2[i] = 0x20
    end

    Characters{N, M}(v2)
end

function Characters{N}(itr) where {N}
    Characters{N, UInt8}(itr)
end

Characters(s::Characters) = s

function Characters(s::AbstractString)
    isempty(s) && return missing
    sl = cld(sizeof(s), length(s))
    if  sl == 1
        Characters{length(s), UInt8}(s)
    else
        Characters{length(s), UInt16}(s)
    end
    # else
    #     throw(ArgumentError("Characters only support UInt8 and UInt16"))
    # end
end

macro c_str(str)
    Characters(str)
end

function Base.print(io::IO, s::Characters)
    # s_end = length(s)
    # @inbounds for i in length(s):-1:1
    #     s.data[i] == 0x20 ? s_end -= 1 : break
    # end
    print(io, String(view(s, 1:length(s))))
end
Base.string(s::Characters) = String(s)

function Base.:(==)(s1::Characters, s2::Characters)
    # s1end = length(s1)
    # s2end = length(s2)
    # @inbounds for i in length(s1):-1:1
    #     s1.data[i] == 0x20 ? s1end -= 1 : break
    # end
    # @inbounds for i in length(s2):-1:1
    #     s2.data[i] == 0x20 ? s2end -= 1 : break
    # end
    # s1end != s2end && return false
    # @inbounds for i in 1:s1end
    #     s1.data[i] != s2.data[i] && return false
    # end
    # return true
    # return view(s1, 1:length(s1)) == view(s2, 1:length(s2))
    cmp(s1,s2) == 0
end

function Base.:(==)(s1::Characters, s2::AbstractString)
    # M = max(N, length(s2))
    s1 == Characters(s2)
end
Base.:(==)(s1::AbstractString, s2::Characters) = s2 == s1

Base.isequal(s1::Characters, s2::Characters) =  cmp(s1, s2) == 0#s1 == s2
function Base.isequal(s1::Characters, s2::AbstractString)
    # M = max(N, length(s2))
    isequal(Characters(s1), Characters(s2))
end
Base.isequal(s1::AbstractString, s2::Characters) = isequal(s2, s1)

function Base.isless(s1::Characters, s2::Characters)
    # s1end = length(s1)
    # s2end = length(s2)
    # @inbounds for i in length(s1):-1:1
    #     s1.data[i] == 0x20 ? s1end -= 1 : break
    # end
    # @inbounds for i in length(s2):-1:1
    #     s2.data[i] == 0x20 ? s2end -= 1 : break
    # end
    # isless(view(s1, 1:length(s1)), view(s2, 1:length(s2)))
    cmp(s1,s2)<0
end


function Base.isless(s1::Characters, s2::AbstractString)
    # M = max(N, length(s2))
    isless(s1, Characters(s2))
end
function Base.isless(s1::AbstractString, s2::Characters)
    # M = max(N, length(s1))
    isless(Characters(s1), s2)
end

function iterate(s::Characters{N}, i::Int = 1) where N
    i > length(s) && return nothing
    return (Char.(s.data[i]), i+1)
end

lastindex(s::Characters{N}) where {N} = length(s)

getindex(s::Characters, i::Int) = Char(s.data[i])

sizeof(s::Characters) = sizeof(s.data)
sizeof(::Type{Characters{N}}) where N = N

function length(s::Characters)
    s_end = length(s.data)
    @inbounds for i in length(s.data):-1:1
        s.data[i] == 0x20 ? s_end -= 1 : break
    end
    return s_end
end


ncodeunits(s::Characters) = length(s.data)

codeunit(::Type{Characters{N, M}}) where N where M = M
codeunit(::Type{Characters{N}}) where N = UInt8
codeunit(s::Characters, i::Integer) = s.data[i]

isvalid(s::Characters, i::Int) = checkbounds(Bool, s, i)

Characters(s::Symbol) = Character(string(s))

Characters(::Missing) = missing
Characters{N}(::Missing) where N = missing
Characters{N, M}(::Missing) where N where M = missing

function read(io::IO, T::Type{Characters{N, M}}) where N where M
    return read!(io, Ref{T}())[]::T
end

function write(io::IO, s::Characters{N, M}) where N where M
    return write(io, Ref(s))
end

# TODO I don't know how I should do this for UInt16
function Base.hash(s::Characters{N, UInt8}, h::UInt) where N
    h += Base.memhash_seed
    ref = Ref(s.data)
    ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), ref, length(s), h % UInt32) + h
end


function Base.cmp(a::Characters, b::Characters)
    a === b && return 0
    a, b = Iterators.Stateful(a), Iterators.Stateful(b)
    for (c, d) in zip(a, b)
        c ≠ d && return ifelse(c < d, -1, 1)
    end
    isempty(a) && return ifelse(isempty(b), 0, -1)
    return 1
end
