# forked from FixedSizeStrings.jl

import Base: iterate, lastindex, getindex, sizeof, length, ncodeunits, codeunit, isvalid, read, write, setindex!, string, convert

struct Characters{N} <: AbstractString
    data::NTuple{N,UInt16}
    Characters{N}(itr) where {N} = new(NTuple{N,UInt16}(rpad(itr, N)))
end

Characters(s::AbstractString) = Characters{length(s)}(s)

Base.string(s::Characters) = join(Char.(s.data))

Base.:(==)(s1::Characters, s2::Characters) = s1.data == s2.data
function Base.:(==)(s1::Characters{N}, s2::AbstractString) where N
    M = max(N, length(s2))
    Characters{M}(s1) == Characters{M}(s2)
end
Base.:(==)(s1::AbstractString, s2::Characters) = s2 == s1

Base.isequal(s1::Characters, s2::Characters) = isequal(s1.data, s2.data)
function Base.isequal(s1::Characters{N}, s2::AbstractString) where N
    M = max(N, length(s2))
    isequal(Characters{M}(s1), Characters{M}(s2))
end
Base.isequal(s1::AbstractString, s2::Characters) = isequal(s2, s1)

Base.isless(s1::Characters, s2::Characters) = isless(s1.data, s2.data)
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

codeunit(::Characters) = UInt16
codeunit(s::Characters, i::Integer) = s.data[i]

isvalid(s::Characters, i::Int) = checkbounds(Bool, s, i)

function read(io::IO, T::Type{Characters{N}}) where N
    return read!(io, Ref{T}())[]::T
end

function write(io::IO, s::Characters{N}) where N
    return write(io, Ref(s))
end
