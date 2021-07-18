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

# this will allocate, we need to solve this
function _process_one_line!(res, lbuff, types, dlm, charcount, current_line)
    cc = 1
    # if current_line[] % 10000 == 0
    #     @show current_line[]
    # end
    for j in 1:length(types)
        nd = findnext(isequal(dlm), lbuff.data, cc)
        nd === nothing || nd > charcount ? en = charcount  : en = nd - 1
        ourparser(res[j], lbuff, cc, en, current_line, types[j])
        cc = en + 1 + length(dlm)
    end
end

function _process_iobuff!(res, iobuff, lbuff, types, dlm, eol, cnt_read_bytes, iobuffsize, lbuffsize, current_line)
    cnt = 1
    cnt_buff = 1
    lastvalid = 0
    while cnt_buff <= cnt_read_bytes
        if iobuff[cnt_buff] !== eol
            lbuff.data[cnt] = iobuff[cnt_buff]
            cnt += 1
        else
            _process_one_line!(res, lbuff, types, dlm, cnt - 1, current_line)
            current_line[] += 1
            cnt = 1
        end
        if cnt_buff == cnt_read_bytes
            _process_one_line!(res, lbuff, types, dlm, cnt - 1, current_line)
            current_line[] += 1
        end

         cnt_buff += 1
    end
end


function readfile(path, types, n; informats, missingstrings, header = true, delimeter = ',', linebreak = '\n', lbuffsize = 32000, buffsize = 10^6)
    f = open(path, "r")
    res = [Vector{Union{Missing, intypes[i]}}(undef, n) for i in 1:length(intypes)]
    dlm = UInt8(delimeter)
    eol = UInt8(linebreak)

    buffer = Vector{UInt8}(undef, buffsize)
    lbuff = LineBuffer(Vector{UInt8}(undef, lbuffsize))
    # we may need this one we want to map a function before parsing it
    # lbuff_cpy = LineBuffer(Vector{UInt8}(undef, lbuffsize))

    if header
        readline(f)
    end
    current_line = Ref{Int}(1)
    cnt = 1
    last_line_complete = true
    while true
      cnt_read_bytes = readbytes!(f, buffer)
        if !eof(f)
            if buffer[end] !== eol
                last_line_complete = false
            end
        end
      # check if this is the last chunk?
      # if not, the last line may not be read totally, so we need to take care of iterate
      # now we should process it
        _process_iobuff!(res, iobuff, lbuff, types, dlm, eol, cnt_read_bytes, iobuffsize, lbuffsize, current_line)
        # we need to break at some point
        break
    end
    close(f)
    outres
end
