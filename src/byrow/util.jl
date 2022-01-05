function mapreduce_index(f::Vector{<:Function}, op, itr, init)
    y = iterate(itr)
    y === nothing && return init
    v = op(init, (y[1]), f[y[2][2]])
    while true
        y = iterate(itr, y[2])
        y === nothing && break
        v = op(v, (y[1]), f[y[2][2]])
    end
    return v
end

# these functions are experimental and later must be moved to row_functions or hp_row_functions
function row_any_multi(ds::AbstractDataset, f::Vector{<:Function}, cols = :)
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "number of provided functions must match the number of selected columns"
    _op_bool_add_multi(x::Bool,y::Bool) = x | y ? true : false
    op_for_any_multi!(x, y, f) = x .= _op_bool_add_multi.(x, f.(y))
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce_index(f, op_for_any_multi!, view(_columns(ds),colsidx), zeros(Bool, size(ds,1)))
end

function row_all_multi(ds::AbstractDataset, f::Vector{<:Function}, cols = :)
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "number of provided functions must match the number of selected columns"
    _op_bool_mult_multi(x::Bool,y::Bool) = x & y ? true : false
    op_for_all_multi!(x, y, f) = x .= _op_bool_mult_multi.(x, f.(y))
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce_index(f, op_for_all_multi!, view(_columns(ds),colsidx), ones(Bool, size(ds,1)))
end

_op_bool_add_multi(x::Bool,y::Bool) = x || y ? true : false

function hp_bool_add_multi!(x, y, f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _op_bool_add_multi(x[i], f(y[i]))
    end
    x
end

function hp_row_any_multi(ds::AbstractDataset, f::Vector{<:Function}, cols = :)
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "number of provided functions must match the number of selected columns"
    _hp_op_for_any_multi!(x, y, f) = x .= hp_bool_add_multi!(x, y, f)
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce_index(f, _hp_op_for_any_multi!, view(_columns(ds),colsidx), zeros(Bool, size(ds,1)))
end

_op_bool_mult_multi(x::Bool,y::Bool) = x && y ? true : false

function hp_bool_mult_multi!(x, y, f)
    Threads.@threads for i in 1:length(x)
        @inbounds x[i] = _op_bool_mult_multi(x[i], f(y[i]))
    end
    x
end

function hp_row_all_multi(ds::AbstractDataset, f::Vector{<:Function}, cols = :)
    colsidx = index(ds)[cols]
    @assert length(f) == length(colsidx) "number of provided functions must match the number of selected columns"
    _hp_op_for_all_multi!(x, y, f) = x .= hp_bool_mult_multi!(x, y, f)
    # mapreduce(identity, op_for_anymissing!, eachcol(ds)[colsidx[sel_colsidx]], init = zeros(Bool, size(ds,1)))
    mapreduce_index(f, _hp_op_for_all_multi!, view(_columns(ds),colsidx), ones(Bool, size(ds,1)))
end

__STRING(x) = string(x)
__STRING(x::AbstractString) = x
__STRING(::Missing) = ""
__STRING(x::Bool) = x ? "1" : "0"
__STRING(::Nothing) = ""

__CODEUNIT(x) = Base.CodeUnits(__STRING(x))



#
# function _op_for_row_join(x, y, f, delim, quotechar, idx, p)
#     idx[] += 1
#     if quotechar === nothing
#         if idx[] < p
#             x .*= STRING.(f[idx[]].(y))
#             x .*= delim
#         else
#             x .*= STRING.(f[idx[]].(y))
#             x .*= '\n'
#         end
#     else
#         if idx[] < p
#             x .*= quotechar
#             x .*= STRING.(f[idx[]].(y))
#             x .*= quotechar
#             x .*= delim
#         else
#             x .*= quotechar
#             x .*= STRING.(f[idx[]].(y))
#             x .*= quotechar
#             x .*= '\n'
#         end
#     end
#     x
# end
#
# function row_join(ds::AbstractDataset, f::Vector{<:Function}, cols  = :; delim = ',', quotechar = nothing)
#     colsidx = index(ds)[cols]
#     idx = Ref{Int}(0)
#     p = length(colsidx)
#     init0 = fill("", nrow(ds))
#     mapreduce(identity, (x,y)->_op_for_row_join(x,y,f, delim, quotechar, idx, p), view(_columns(ds), colsidx), init = init0)
# end

# some experimental ideas

function write_vals!(a, pos, x::Integer)
    n_positive, neg = Base.split_sign(x)
    #always we assume base = 10
    needed_space = neg + ndigits(x)
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    _base!(a, pos, 10, n_positive, 0, neg)
    pos+needed_space
end

#TODO this doesn't work - since in writeshortest() a must be a vector{UInt8} but we pass a view of a matrix in DLMReader
function write_vals!(a, pos, x::Union{Float16, Float32, Float64})
    needed_space = Base.Ryu.neededdigits(typeof(x))
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    _writeshortest(a, pos, x)
end

function write_vals!(a, pos, x::Bool)
    needed_space = 1
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    x ? a[pos] = UInt8('1') : a[pos] = UInt8('0')
    pos+1
end

function write_vals!(a, pos, ::Missing)
    pos
end
function write_vals!(a, pos, ::Nothing)
    pos
end


function write_vals!(a, pos, x::AbstractString)
    needed_space = length(x)
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    z = Base.CodeUnits(x)
    for i in 1:length(x)
        a[i+pos-1] =z[i]
    end
    pos+length(x)
end


function write_vals!(a, pos, x)
    y = string(x)
    needed_space = length(y)
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    for i in 1:length(y)
        a[i+pos-1] = Base.CodeUnits(y)[i]
    end
    pos+length(y)
end

function write_delim!(a, pos, delim)
    needed_space = length(delim)
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    for i in 1:length(delim)
        a[i+pos-1] = delim[i]
    end
    pos+length(delim)
end

function write_eol!(a, pos)
    needed_space = 1
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    a[pos] = UInt8('\n')
    pos+1
end

function write_quotechar!(a, pos, quotechar)
    needed_space = 1
    available_space = length(a)-pos+1
    needed_space > available_space && throw(ArgumentError("not enough space in buffer to write value into it"))
    a[pos] = quotechar
    pos+1
end
function write_quotechar!(a, pos, ::Nothing)
    pos
end


function _base!(a, pos, base::Integer, x::Integer, pad::Int, neg::Bool)
    (x >= 0) | (base < 0) || throw(DomainError(x, "For negative `x`, `base` must be negative."))
    2 <= abs(base) <= 62 || throw(DomainError(base, "base must satisfy 2 ≤ abs(base) ≤ 62"))
    b = (base % Int)::Int
    digits = abs(b) <= 36 ? Base.base36digits : Base.base62digits
    # pad = 0 makes issue when x == 0 (n will be 0)
    n = neg + ndigits(x, base=b)
    i = n
    @inbounds while i > neg
        if b > 0
            a[i+pos-1] = digits[1 + (rem(x, b) % Int)::Int]
            x = div(x,b)
        else
            a[i+pos-1] = digits[1 + (mod(x, -b) % Int)::Int]
            x = cld(x,b)
        end
        i -= 1
    end
    if neg; @inbounds a[pos]=0x2d; end
    a
end

function _op_for_row_join!(buffer, currentpos, y, f, delim, quotechar, idx, p, lo, hi)
    idx[] += 1
    if quotechar === nothing
        if idx[] < p
            for i in lo:hi
                currentpos[i] = write_vals!(view(buffer, :, i), currentpos[i], f[idx[]](y[i]))
                currentpos[i] = write_delim!(view(buffer, :, i), currentpos[i], delim)
            end
        else
            for i in lo:hi
                currentpos[i] = write_vals!(view(buffer, :, i), currentpos[i], f[idx[]](y[i]))
                currentpos[i] = write_eol!(view(buffer, :, i), currentpos[i])
            end
        end
    else
        if nonmissingtype(eltype(y)) <: AbstractString
            quotecharval = UInt8(quotechar)
        else
            quotecharval = nothing
        end
        if idx[]<p
            for i in lo:hi
                currentpos[i] = write_quotechar!(view(buffer, :, i), currentpos[i], quotecharval)
                currentpos[i] = write_vals!(view(buffer, :, i), currentpos[i], f[idx[]](y[i]))
                currentpos[i] = write_quotechar!(view(buffer, :, i), currentpos[i], quotecharval)
                currentpos[i] = write_delim!(view(buffer, :, i), currentpos[i], delim)
            end
        else
            for i in lo:hi
                currentpos[i] = write_quotechar!(view(buffer, :, i), currentpos[i], quotecharval)
                currentpos[i] = write_vals!(view(buffer, :, i), currentpos[i], f[idx[]](y[i]))
                currentpos[i] = write_quotechar!(view(buffer, :, i), currentpos[i], quotecharval)
                currentpos[i] = write_eol!(view(buffer, :, i), currentpos[i])
            end
        end
    end
    currentpos
end

# buffer = Matrix{UInt8}(undef, lsize , nrow(ds))
# currentpos = ones(Int, nrow(ds))
function row_join!(buffer, currentpos, ds::AbstractDataset, f::Vector{<:Function}, cols = :; delim = ',', quotechar = nothing, threads = true)
    colsidx = multiple_getindex(index(ds), cols)
    p = length(colsidx)
    dlm = UInt8.(delim)
    if threads
        cz = div(nrow(ds), __NCORES)
        idx = [Ref{Int}(0) for _ in 1:__NCORES]
        Threads.@threads for i in 1:__NCORES
            lo = (i-1)*cz+1
            i == __NCORES ? hi = nrow(ds) : hi = i*cz
            mapreduce(identity, (x,y) -> _op_for_row_join!(buffer, x, y, f, dlm, quotechar, idx[i], p, lo, hi), view(_columns(ds),colsidx), init = currentpos)
        end
    else
        idx = Ref{Int64}(0)
        mapreduce(identity, (x,y) -> _op_for_row_join!(buffer, x, y, f, dlm, quotechar, idx, p, 1, nrow(ds)), view(_columns(ds),colsidx), init = currentpos)
    end
    currentpos, buffer
end



@inline function append_sign(x, plus, space, buf, pos)
    if signbit(x) && !isnan(x)  # suppress minus sign for signaling NaNs
        buf[pos] = UInt8('-')
        pos += 1
    elseif plus
        buf[pos] = UInt8('+')
        pos += 1
    elseif space
        buf[pos] = UInt8(' ')
        pos += 1
    end
    return pos
end

### From Base.Ryu, because we need buf to be View of an array not vector (maybe we should change it in Ryu?)
function _writeshortest(buf, pos, x::T,
                       plus=false, space=false, hash=true,
                       precision=-1, expchar=UInt8('e'), padexp=false, decchar=UInt8('.'),
                       typed=false, compact=false) where {T}
    @assert 0 < pos <= length(buf)
    # special cases
    if x == 0
        if typed && x isa Float16
            buf[pos] = UInt8('F')
            buf[pos + 1] = UInt8('l')
            buf[pos + 2] = UInt8('o')
            buf[pos + 3] = UInt8('a')
            buf[pos + 4] = UInt8('t')
            buf[pos + 5] = UInt8('1')
            buf[pos + 6] = UInt8('6')
            buf[pos + 7] = UInt8('(')
            pos += 8
        end
        pos = append_sign(x, plus, space, buf, pos)
        buf[pos] = UInt8('0')
        pos += 1
        if hash
            buf[pos] = decchar
            pos += 1
        end
        if precision == -1
            buf[pos] = UInt8('0')
            pos += 1
            if typed && x isa Float32
                buf[pos] = UInt8('f')
                buf[pos + 1] = UInt8('0')
                pos += 2
            end
            if typed && x isa Float16
                buf[pos] = UInt8(')')
                pos += 1
            end
            return pos
        end
        while hash && precision > 1
            buf[pos] = UInt8('0')
            pos += 1
            precision -= 1
        end
        if typed && x isa Float32
            buf[pos] = UInt8('f')
            buf[pos + 1] = UInt8('0')
            pos += 2
        end
        if typed && x isa Float16
            buf[pos] = UInt8(')')
            pos += 1
        end
        return pos
    elseif isnan(x)
        pos = append_sign(x, plus, space, buf, pos)
        buf[pos] = UInt8('N')
        buf[pos + 1] = UInt8('a')
        buf[pos + 2] = UInt8('N')
        if typed
            if x isa Float32
                buf[pos + 3] = UInt8('3')
                buf[pos + 4] = UInt8('2')
            elseif x isa Float16
                buf[pos + 3] = UInt8('1')
                buf[pos + 4] = UInt8('6')
            end
        end
        return pos + 3 + (typed && x isa Union{Float32, Float16} ? 2 : 0)
    elseif !isfinite(x)
        pos = append_sign(x, plus, space, buf, pos)
        buf[pos] = UInt8('I')
        buf[pos + 1] = UInt8('n')
        buf[pos + 2] = UInt8('f')
        if typed
            if x isa Float32
                buf[pos + 3] = UInt8('3')
                buf[pos + 4] = UInt8('2')
            elseif x isa Float16
                buf[pos + 3] = UInt8('1')
                buf[pos + 4] = UInt8('6')
            end
        end
        return pos + 3 + (typed && x isa Union{Float32, Float16} ? 2 : 0)
    end

    output, nexp = Base.Ryu.reduce_shortest(x, compact ? 999_999 : nothing)

    if typed && x isa Float16
        buf[pos] = UInt8('F')
        buf[pos + 1] = UInt8('l')
        buf[pos + 2] = UInt8('o')
        buf[pos + 3] = UInt8('a')
        buf[pos + 4] = UInt8('t')
        buf[pos + 5] = UInt8('1')
        buf[pos + 6] = UInt8('6')
        buf[pos + 7] = UInt8('(')
        pos += 8
    end
    pos = append_sign(x, plus, space, buf, pos)

    olength = Base.Ryu.decimallength(output)
    exp_form = true
    pt = nexp + olength
    if -4 < pt <= (precision == -1 ? (T == Float16 ? 3 : 6) : precision) &&
        !(pt >= olength && abs(mod(x + 0.05, 10^(pt - olength)) - 0.05) > 0.05)
        exp_form = false
        if pt <= 0
            buf[pos] = UInt8('0')
            pos += 1
            buf[pos] = decchar
            pos += 1
            for _ = 1:abs(pt)
                buf[pos] = UInt8('0')
                pos += 1
            end
            # elseif pt >= olength
            # nothing to do at this point
            # else
            # nothing to do at this point
        end
    else
        pos += 1
    end
    i = 0
    ptr = pointer(buf)
    ptr2 = pointer(Base.Ryu.DIGIT_TABLE)
    if (output >> 32) != 0
        q = output ÷ 100000000
        output2 = (output % UInt32) - UInt32(100000000) * (q % UInt32)
        output = q

        c = output2 % UInt32(10000)
        output2 = div(output2, UInt32(10000))
        d = output2 % UInt32(10000)
        c0 = (c % 100) << 1
        c1 = (c ÷ 100) << 1
        d0 = (d % 100) << 1
        d1 = (d ÷ 100) << 1
        Base.Ryu.memcpy(ptr, pos + olength - 2, ptr2, c0 + 1, 2)
        Base.Ryu.memcpy(ptr, pos + olength - 4, ptr2, c1 + 1, 2)
        Base.Ryu.memcpy(ptr, pos + olength - 6, ptr2, d0 + 1, 2)
        Base.Ryu.memcpy(ptr, pos + olength - 8, ptr2, d1 + 1, 2)
        i += 8
    end
    output2 = output % UInt32
    while output2 >= 10000
        c = output2 % UInt32(10000)
        output2 = div(output2, UInt32(10000))
        c0 = (c % 100) << 1
        c1 = (c ÷ 100) << 1
        Base.Ryu.memcpy(ptr, pos + olength - i - 2, ptr2, c0 + 1, 2)
        Base.Ryu.memcpy(ptr, pos + olength - i - 4, ptr2, c1 + 1, 2)
        i += 4
    end
    if output2 >= 100
        c = (output2 % UInt32(100)) << 1
        output2 = div(output2, UInt32(100))
        Base.Ryu.memcpy(ptr, pos + olength - i - 2, ptr2, c + 1, 2)
        i += 2
    end
    if output2 >= 10
        c = output2 << 1
        buf[pos + 1] = Base.Ryu.DIGIT_TABLE[c + 2]
        buf[pos - exp_form] = Base.Ryu.DIGIT_TABLE[c + 1]
    else
        buf[pos - exp_form] = UInt8('0') + (output2 % UInt8)
    end

    if !exp_form
        if pt <= 0
            pos += olength
            precision -= olength
            while hash && precision > 0
                buf[pos] = UInt8('0')
                pos += 1
                precision -= 1
            end
        elseif pt >= olength
            pos += olength
            precision -= olength
            for _ = 1:nexp
                buf[pos] = UInt8('0')
                pos += 1
                precision -= 1
            end
            if hash
                buf[pos] = decchar
                pos += 1
                if precision < 0
                    buf[pos] = UInt8('0')
                    pos += 1
                end
                while precision > 0
                    buf[pos] = UInt8('0')
                    pos += 1
                    precision -= 1
                end
            end
        else
            pointoff = olength - abs(nexp)
            Base.Ryu.memmove(ptr, pos + pointoff + 1, ptr, pos + pointoff, olength - pointoff + 1)
            buf[pos + pointoff] = decchar
            pos += olength + 1
            precision -= olength
            while hash && precision > 0
                buf[pos] = UInt8('0')
                pos += 1
                precision -= 1
            end
        end
        if typed && x isa Float32
            buf[pos] = UInt8('f')
            buf[pos + 1] = UInt8('0')
            pos += 2
        end
    else
        if olength > 1 || hash
            buf[pos] = decchar
            pos += olength
            precision -= olength
        end
        if hash && olength == 1
            buf[pos] = UInt8('0')
            pos += 1
        end
        while hash && precision > 0
            buf[pos] = UInt8('0')
            pos += 1
            precision -= 1
        end

        buf[pos] = expchar
        pos += 1
        exp2 = nexp + olength - 1
        if exp2 < 0
            buf[pos] = UInt8('-')
            pos += 1
            exp2 = -exp2
        elseif padexp
            buf[pos] = UInt8('+')
            pos += 1
        end

        if exp2 >= 100
            c = exp2 % 10
            Base.Ryu.memcpy(ptr, pos, ptr2, 2 * div(exp2, 10) + 1, 2)
            buf[pos + 2] = UInt8('0') + (c % UInt8)
            pos += 3
        elseif exp2 >= 10
            Base.Ryu.memcpy(ptr, pos, ptr2, 2 * exp2 + 1, 2)
            pos += 2
        else
            if padexp
                buf[pos] = UInt8('0')
                pos += 1
            end
            buf[pos] = UInt8('0') + (exp2 % UInt8)
            pos += 1
        end
    end
    if typed && x isa Float16
        buf[pos] = UInt8(')')
        pos += 1
    end

    return pos
end
