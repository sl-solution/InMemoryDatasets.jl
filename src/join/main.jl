function DataAPI.leftjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, check = true)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_left(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_left(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function DataAPI.rightjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, check = true)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_left(dsr, dsl, onleft = onright, onright = onleft, makeunique = makeunique, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_left(dsr, dsl, onleft = onright, onright = onleft, makeunique = makeunique, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function DataAPI.innerjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, check = true)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_inner(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_inner(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function DataAPI.outerjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, check = true)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_outer(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_outer(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function DataAPI.antijoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, check = true)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_anti(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_anti(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function asofjoin(dsl::Dataset, dsr::Dataset; on = nothing, direction = :backward, makeunique = false)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        length(onleft) > 1 && throw(ArgumentError("for `asofjoin` only one column must be specified for the `on` keyword"))
        if direction == :backward
            _join_asofback(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique)
        elseif direction == :forward
            _join_asoffor(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique)
        else
            throw(ArgumentError("`direction` can be only :backward or :forward"))
        end

    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        length(onleft) > 1 && throw(ArgumentError("for `asofjoin` only one column must be specified for the `on` keyword"))
        if direction == :backward
            _join_asofback(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique)
        elseif direction == :forward
            _join_asoffor(dsl, dsr, onleft = onleft, onright = onright, makeunique = makeunique)
        else
            throw(ArgumentError("`direction` can be only :backward or :forward"))
        end
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end