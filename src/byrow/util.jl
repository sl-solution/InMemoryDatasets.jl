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

STRING(x) = string(x)
STRING(::Missing) = ""
STRING(x::Bool) = x ? "1" : "0"
STRING(::Nothing) = ""

function _op_for_row_join(x, y, f, delim, quotechar, idx, p)
    idx[] += 1
    if quotechar === nothing
        if idx[] < p
            x .*= STRING.(f[idx[]].(y))
            x .*= delim
        else
            x .*= STRING.(f[idx[]].(y))
            x .*= '\n'
        end
    else
        if idx[] < p
            x .*= quotechar
            x .*= STRING.(f[idx[]].(y))
            x .*= quotechar
            x .*= delim
        else
            x .*= quotechar
            x .*= STRING.(f[idx[]].(y))
            x .*= quotechar
            x .*= '\n'
        end
    end
    x
end

function row_join(ds::AbstractDataset, f::Vector{<:Function}, cols  = :; delim = ',', quotechar = nothing)
    colsidx = index(ds)[cols]
    idx = Ref{Int}(0)
    p = length(colsidx)
    init0 = fill("", nrow(ds))
    mapreduce(identity, (x,y)->_op_for_row_join(x,y,f, delim, quotechar, idx, p), view(_columns(ds), colsidx), init = init0)
end
