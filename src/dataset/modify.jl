struct MultiCol
    x
end
function byrow(@nospecialize(f); @nospecialize(args...))
    br = :($f, $args)
    br.head = :BYROW
    br
end
# col => fun => dst, the job is to create col => fun => :dst
function normalize_modify(idx::Index,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Pair{<:Union{Base.Callable},
                                                        <:Union{Symbol, AbstractString}}})
                                                        )
    src, (fun, dst) = sel
    return idx[src] => fun => Symbol(dst)
end
# col => fun => dst, the job is to create col => fun => :dst
function normalize_modify(idx::Index,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Pair{<:Union{Base.Callable},
                                                        <:Vector{<:Union{Symbol, AbstractString}}}})
                                                        )
    src, (fun, dst) = sel
    return idx[src] => fun => MultiCol(Symbol.(dst))
end
# col => fun, the job is to create col => fun => :colname
function normalize_modify(idx::Index,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Union{Base.Callable}}))

    src, fun = sel
    return idx[src] => fun => _names(idx)[idx[src]]
end

# cols => fun, the job is to create [col1 => fun => :col1name, col2 => fun => :col2name ...]
function normalize_modify(idx::Index,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Union{Base.Callable,Expr}}))
    colsidx = idx[sel.first]
    if sel.second isa Expr
        if sel.second.head == :BYROW
            # TODO needs a better name for destination
            return idx[colsidx] => sel.second => Symbol("row_", funname(sel.second.args[1]))
        end
    end
    res = [normalize_modify(idx, colsidx[1] => sel.second)]
    for i in 2:length(colsidx)
        push!(res, normalize_modify(idx, colsidx[i] => sel.second))
    end
    return res
end

# special case cols => byrow(...) => :name
function normalize_modify(idx::Index,
    @nospecialize(sel::Pair{<:MultiColumnIndex,
                            <:Pair{<:Expr,
                                <:Union{Symbol, AbstractString}}}))
    colsidx = idx[sel.first]
    if sel.second.first.head == :BYROW
        return idx[colsidx] => sel.second.first => Symbol(sel.second.second)
    else
        throw(ArgumentError("only byrow operation is supported for cols => fun => :name"))
    end
end

# cols .=> fun .=> dsts, the job is to create col1 => fun => :dst1, col2 => fun => :dst2, ...
function normalize_modify(idx::Index,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Pair{<:Union{Base.Callable,Expr},
                                                        <:AbstractVector{<:Union{Symbol, AbstractString}}}}))
    colsidx = idx[sel.first]
    if !(length(colsidx) == length(sel.second.second))
        throw(ArgumentError("The input number of columns and the length of the output names should match"))
    end
    @show typeof(sel.second.first) isa Expr
    if typeof(sel.second.first) == Expr
        if sel.second.first.head == :BYROW
            throw(ArgumentError("in byrow operation the destination name cannot be more than one"))
        end
    end
    res = [normalize_modify(idx, colsidx[1] => sel.second.first => sel.second.second[1])]
    for i in 2:length(colsidx)
        push!(res, normalize_modify(idx, colsidx[i] => sel.second.first => sel.second.second[i]))
    end
    return res
end

function normalize_modify_multiple(idx::Index, @nospecialize(args...))
    res = Any[]
    for i in 1:length(args)
        _res = normalize_modify(idx, args[i])
        if typeof(_res) <: Pair
            push!(res, _res)
        else
            for j in 1:length(_res)
                push!(res, _res[j])
            end
        end
    end
    res
end




# function modify(ds::Dataset, @nospecialize(args...))
