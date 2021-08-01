# forked from FixedSizeStrings.jl

import Base: iterate, lastindex, getindex, sizeof, length, ncodeunits, codeunit, isvalid, read, write, setindex!, string, convert

struct Characters{N, M} <: AbstractString
    data::NTuple{N, M}
    Characters{N, M}(itr) where {N} where {M} = new(NTuple{N, M}(rpad(itr, N)))
end

Characters{N}(itr) where {N} = Characters{N, UInt8}(itr)

function Characters(s::AbstractString)
    sl = cld(sizeof(s), length(s))
    if  sl == 1
        Characters{length(s), UInt8}(s)
    elseif sl == 2
        Characters{length(s), UInt16}(s)
    else
        throw(ArgumentError("Characters only support UTF-8 and UTF-16"))
    end
end

macro c_str(str)
    Characters(str)
end

Base.string(s::Characters) = join(Char.(s.data))

function Base.:(==)(s1::Characters, s2::Characters)
    s1end = length(s1)
    s2end = length(s2)
    @inbounds for i in length(s1):-1:1
        s1.data[i] == 0x20 ? s1end -= 1 : break
    end
    @inbounds for i in length(s2):-1:1
        s2.data[i] == 0x20 ? s2end -= 1 : break
    end
    s1end != s2end && return false
    @inbounds for i in 1:s1end
        s1.data[i] != s2.data[i] && return false
    end
    return true
end

function Base.:(==)(s1::Characters{N}, s2::AbstractString) where N
    M = max(N, length(s2))
    Characters{M}(s1) == Characters{M}(s2)
end
Base.:(==)(s1::AbstractString, s2::Characters) = s2 == s1

Base.isequal(s1::Characters, s2::Characters) = s1 == s2
function Base.isequal(s1::Characters{N}, s2::AbstractString) where N
    M = max(N, length(s2))
    isequal(Characters{M}(s1), Characters{M}(s2))
end
Base.isequal(s1::AbstractString, s2::Characters) = isequal(s2, s1)

function Base.isless(s1::Characters, s2::Characters)
    s1end = length(s1)
    s2end = length(s2)
    @inbounds for i in length(s1):-1:1
        s1.data[i] == 0x20 ? s1end -= 1 : break
    end
    @inbounds for i in length(s2):-1:1
        s2.data[i] == 0x20 ? s2end -= 1 : break
    end
    isless(view(s1, 1:s1end), view(s2, 1:s2end))
end


function Base.isless(s1::Characters{N}, s2::AbstractString) where N
    M = max(N, length(s2))
    isless(Characters{M}(s1), Characters{M}(s2))
end
function Base.isless(s1::AbstractString, s2::Characters{N}) where N
    M = max(N, length(s1))
    isless(Characters{M}(s1), Characters{M}(s2))
end

function iterate(s::Characters{N}, i::Int = 1) where N
    i > N && return nothing
    return (Char(s.data[i]), i+1)
end

lastindex(s::Characters{N}) where {N} = N

getindex(s::Characters, i::Int) = Char(s.data[i])

sizeof(s::Characters) = sizeof(s.data)

length(s::Characters) = length(s.data)

ncodeunits(s::Characters) = length(s.data)

codeunit(::Characters{N, M}) where N where M = M
codeunit(s::Characters, i::Integer) = s.data[i]

isvalid(s::Characters, i::Int) = checkbounds(Bool, s, i)

Characters(s::Symbol) = Character(string(s))

Characters(::Missing) = missing
Characters{N}(::Missing) where N = missing
Characters{N, M}(::Missing) where N where M = missing

const memhash = UInt === UInt64 ? :memhash_seed : :memhash32_seed
const memhash_seed = UInt === UInt64 ? 0x71e729fd56419c81 : 0x56419c81

# this needs more work
function hash(s::Characters, h::UInt)
    s_end = length(s)
    @inbounds for i in length(s):-1:1
        s.data[i] == 0x20 ? s_end -= 1 : break
    end
    h += memhash_seed
    ccall(memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), s, sizeof(view(s, 1:s_end)), h % UInt32) + h
end

function read(io::IO, T::Type{Characters{N, M}}) where N where M
    return read!(io, Ref{T}())[]::T
end

function write(io::IO, s::Characters{N, M}) where N where M
    return write(io, Ref(s))
end
