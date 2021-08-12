# Initial Idea, far from usability
# DO NOT use it
struct LineBuffer <: AbstractString
    data::Vector{UInt8}
end

function Base.iterate(s::LineBuffer, i::Int = 1)
    i > length(s.data) && return nothing
    return (Char(s.data[i]), i+1)
end

Base.lastindex(s::LineBuffer) = length(s.data)

Base.getindex(s::LineBuffer, i::Int) = s.data[i]

Base.sizeof(s::LineBuffer) = sizeof(s.data)

Base.length(s::LineBuffer) = length(s.data)

Base.ncodeunits(s::LineBuffer) = length(s.data)

Base.codeunit(::LineBuffer) = UInt8
Base.codeunit(s::LineBuffer, i::Integer) = s.data[i]

Base.isvalid(s::LineBuffer, i::Int) = checkbounds(Bool, s, i)

function ourparser(res, lbuff, cc, nd, current_line, ::Type{T}) where T <: Integer
    # val = Base.tryparse_internal(T, lbuff, cc, nd, 10, false)
    hasvalue, val = ccall(:jl_try_substrtod, Tuple{Bool, Float64},
    (Ptr{UInt8},Csize_t,Csize_t), lbuff.data, cc-1, nd - cc +1)
    hasvalue ? res[current_line[]] = T(val) : res[current_line[]] = missing
end

function ourparser(res, lbuff, cc, nd, current_line, ::Type{Float64})
    hasvalue, val = ccall(:jl_try_substrtod, Tuple{Bool, Float64},
    (Ptr{UInt8},Csize_t,Csize_t), lbuff.data, cc-1, nd - cc +1)
    hasvalue ? res[current_line[]] = val : res[current_line[]] = missing
end
function ourparser(res, lbuff, cc, nd, current_line, ::Type{Float32})
    hasvalue, val = ccall(:jl_try_substrtof, Tuple{Bool, Float32},
    (Ptr{UInt8},Csize_t,Csize_t), lbuff.data, cc-1, nd - cc +1)
    hasvalue ? res[current_line[]] = val : res[current_line[]] = missing
end
function ourparser(res, lbuff, cc, nd, current_line, char_buf, ::Type{T}) where T <: Characters
    res[current_line[]] = Characters{8, UInt8}(lbuff.data, char_buf, cc, nd)
end

# this will allocate, we need to solve this
function _process_one_line!(res, lbuff, types, dlm, charcount, current_line, char_buf)
    cc = 1
    # if current_line[] % 10000 == 0
    #     @show current_line[]
    # end
    for j in 1:length(types)
        nd = findnext(isequal(dlm), lbuff.data, cc)
        nd === nothing || nd > charcount ? en = charcount  : en = nd - 1
        if types[j] == Int64
            ourparser(res[j]::Vector{Union{Missing, Int64}}, lbuff, cc, en, current_line, Int64)
        elseif types[j] == Int32
            ourparser(res[j]::Vector{Union{Missing, Int32}}, lbuff, cc, en, current_line, Int32)
        elseif types[j] == Int16
            ourparser(res[j]::Vector{Union{Missing, Int16}}, lbuff, cc, en, current_line, Int16)
        elseif types[j] == Int8
            ourparser(res[j]::Vector{Union{Missing, Int8}}, lbuff, cc, en, current_line, Int8)
        elseif types[j] == Float64
            ourparser(res[j]::Vector{Union{Missing, Float64}}, lbuff, cc, en, current_line, Float64)
        elseif types[j] == Float32
            ourparser(res[j]::Vector{Union{Missing, Float32}}, lbuff, cc, en, current_line, Float32)
        elseif types[j] <: AbstractString
            ourparser(res[j]::Vector{Union{Missing, Characters{8, UInt8}}}, lbuff, cc, en, current_line, char_buf, Characters)
        end

        cc = en + 1 + length(dlm)
    end
end

function _process_iobuff!(res, iobuff, lbuff, types, dlm, eol, cnt_read_bytes, iobuffsize, lbuffsize, current_line, last_line_complete, last_line, last_valid_buff, char_buf)
    cnt = 1
    cnt_buff = 1
    lastvalid = 0
    while cnt_buff <= last_valid_buff
        if iobuff[cnt_buff] !== eol
            lbuff.data[cnt] = iobuff[cnt_buff]
            cnt += 1
        else
            _process_one_line!(res, lbuff, types, dlm, cnt - 1, current_line, char_buf)
            current_line[] += 1
            cnt = 1
        end
        cnt_buff += 1
    end
end


function readfile(path, types, n; header = true, delimeter = ',', linebreak = '\n', lbuffsize = 32000, buffsize = 10^6)
    f = open(path, "r")
    res = [Vector{Union{Missing, types[i]}}(undef, n) for i in 1:length(types)]
    dlm = UInt8(delimeter)
    eol = UInt8(linebreak)

    buffer = Vector{UInt8}(undef, buffsize)
    lbuff = LineBuffer(Vector{UInt8}(undef, lbuffsize))
    # we may need this one we want to map a function before parsing it
    # lbuff_cpy = LineBuffer(Vector{UInt8}(undef, lbuffsize))

    if header
        readline(f)
    end
    char_buf = Vector{UInt8}(undef, 8)
    current_line = Ref{Int}(1)
    last_line_complete = true
    last_line = false
    last_valid_buff = buffsize
    while true
        cnt_read_bytes = readbytes!(f, buffer)
        if !eof(f)
            if buffer[end] !== eol
                last_line_complete = false
                back_cnt = 0
                for i in buffsize:-1:1
                    last_valid_buff = i
                    buffer[i] == eol && break
                    back_cnt += 1
                end
                cur_position = position(f)
                seek(f, cur_position - back_cnt)
            else
                last_valid_buff = buffsize
            end
        else
            last_line = true
            if buffer[cnt_read_bytes] !== eol
                @warn "the last line is not ended with new line character"
                for i in cnt_read_bytes:-1:1
                    last_valid_buff = i
                    buffer[i] == eol && break
                end

            else
                last_valid_buff = cnt_read_bytes
            end
        end
        _process_iobuff!(res, buffer, lbuff, types, dlm, eol, cnt_read_bytes, buffsize, lbuffsize, current_line, last_line_complete, last_line, last_valid_buff, char_buf)
        # we need to break at some point
        last_line && break
    end
    close(f)
    res
end
