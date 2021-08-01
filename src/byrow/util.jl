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
