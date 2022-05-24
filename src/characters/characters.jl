# forked from FixedSizeStrings.jl

import Base: iterate, lastindex, getindex, sizeof, length, ncodeunits, codeunit, isvalid, read, write, setindex!, string, convert

struct Characters{N} <: AbstractString
    data::NTuple{N, UInt8}
    function Characters{N}(v::Vector{UInt8}) where N
        new(NTuple{N, UInt8}(v))
    end
    function Characters{N}(itr) where {N}
        isempty(itr) && return missing
        new(NTuple{N, UInt8}(rpad(itr, N)))
    end
end

function Characters{N}(v::Vector{UInt8}, v2) where N

    @simd for i in 1:min(N, length(v))
        @inbounds v2[i] = v[i]
    end
    @simd for i in length(v)+1:N
        @inbounds v2[i] = 0x20
    end

    Characters{N}(v2)
end
function Characters{N}(v::Vector{UInt8}, v2, st, en) where N
    o1 = min(N, en-st+1)
    copyto!(v2, 1, v, st, o1)
    @simd for i in o1+1:N
        @inbounds v2[i] = 0x20
    end

    Characters{N}(v2)
end

Characters(s::Characters) = s

function Characters(s::AbstractString)
    isempty(s) && return missing
    Characters{ncodeunits(s)}(collect(codeunits(s)))
    
end

macro c_str(str)
    Characters(str)
end

function Base.String(s::T) where T <: Characters
    len = ncodeunits(s)
    out = Base._string_n(len)
    ref = Ref{T}(s)
    GC.@preserve ref out begin
        ptr = convert(Ptr{UInt8}, Base.unsafe_convert(Ptr{T}, ref))
        unsafe_copyto!(pointer(out), ptr, len)
    end
    return out
end


function Base.print(io::IO, s::T) where T<:Characters
    print(io, String(s))
end
Base.string(s::Characters) = String(s)

function Base.:(==)(s1::Characters, s2::Characters)
    cmp(s1,s2) == 0
end

function Base.:(==)(s1::Characters, s2::AbstractString)
    return view(codeunits(s1), 1:length(s1)) == codeunits(s2)
    
end
Base.:(==)(s1::AbstractString, s2::Characters) = s2 == s1

Base.isequal(s1::Characters, s2::Characters) =  cmp(s1, s2) == 0#s1 == s2
function Base.isequal(s1::Characters, s2::AbstractString)
    return isequal(view(codeunits(s1), 1:length(s1)), codeunits(s2))

end
Base.isequal(s1::AbstractString, s2::Characters) = isequal(s2, s1)

function Base.isless(s1::Characters, s2::Characters)
    cmp(s1,s2)<0
end


function Base.isless(s1::Characters, s2::AbstractString)
    return isless(view(codeunits(s1), 1:length(s1)), codeunits(s2))
end
function Base.isless(s1::AbstractString, s2::Characters)
    return isless(codeunits(s1), view(codeunits(s2), 1:length(s2)))
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


ncodeunits(s::Characters) = length(s)

codeunit(::Type{Characters{N}}) where N = UInt8
codeunit(::Characters) = UInt8
codeunit(s::Characters, i::Integer) = s.data[i]

isvalid(s::Characters, i::Int) = checkbounds(Bool, s, i)

Characters(s::Symbol) = Characters(string(s))

Characters(::Missing) = missing
Characters{N}(::Missing) where N = missing

function read(io::IO, T::Type{Characters{N}}) where N
    return read!(io, Ref{T}())[]::T
end

function write(io::IO, s::Characters{N}) where N
    return write(io, Ref(s))
end

function Base.hash(s::Characters{N}, h::UInt) where N
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
