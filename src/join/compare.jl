function _fill_index_compare!(x, r)
    @simd for i in r
        x[i] = i
    end
end
function _compare(dsl, dsr, ::Val{T}; onleft, onright, cols_left, cols_right, check = true, mapformats = false, on_mapformats = [true, true], stable = false, alg = HeapSort, accelerate = false, method = :sort, threads = true, eq = isequal, obs_id_name = :obs_id, multiple_match = false, multiple_match_name = :multiple, drop_obs_id = true, makeunique = false) where T
    names_left = names(dsl)[cols_left]
    names_right = names(dsr)[cols_right]
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end

    if onleft == nothing
        n_dsl = nrow(dsl)
        n_dsr = nrow(dsr)
        total_length = max(n_dsl, n_dsr)
        obs_id_left = _missings(T, total_length)
        obs_id_right = _missings(T, total_length)
        _fill_index_compare!(obs_id_left, 1:n_dsl)
        _fill_index_compare!(obs_id_right, 1:n_dsr)
        res = Dataset(x1=obs_id_left, x2=obs_id_right, copycols = false)
        rename!(res, :x1=>Symbol(obs_id_name, "_left"), :x2=>Symbol(obs_id_name, "_right"))
    else
        res = outerjoin(dsl[!, onleft], dsr[!, onright], on = 1:length(onleft) .=> 1:length(onright), check = check, mapformats = on_mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads, obs_id = true, obs_id_name = obs_id_name, multiple_match = multiple_match, multiple_match_name = multiple_match_name, makeunique = makeunique)
        total_length = nrow(res)
        obs_cols = index(res)[[Symbol(obs_id_name, "_left"), Symbol(obs_id_name, "_right")]]
        obs_id_left = _columns(res)[obs_cols[1]]
        obs_id_right = _columns(res)[obs_cols[2]]
    end
    _info_cols = ncol(res)
    for j in 1:length(cols_left)
            fl = identity
            if mapformats[1]
                fl = getformat(dsl, cols_left[j])
            end
            fr = identity
            if mapformats[2]
                fr = getformat(dsr, cols_right[j])
            end
            _left_type = Core.Compiler.return_type(fl, Tuple{eltype(_columns(dsl)[cols_left[j]])})
            _right_type = Core.Compiler.return_type(fr, Tuple{eltype(_columns(dsr)[cols_right[j]])})
            _res = allocatecol(Core.Compiler.return_type(eq, Tuple{_left_type, _right_type}), total_length)

            _compare_barrier_function!(_res, _columns(dsl)[cols_left[j]], _columns(dsr)[cols_right[j]], fl, fr, eq, obs_id_left, obs_id_right, threads)

            push!(_columns(res), _res)
            push!(index(res),  Symbol(names_left[j]* "=>" * names_right[j]))
    end
    if drop_obs_id
        select!(res, Not([Symbol(obs_id_name, "_left"), Symbol(obs_id_name, "_right")]))
    end
    res
end


function _compare_barrier_function!(_res, xl, xr, fl, fr, eq_fun, obs_id_left, obs_id_right, threads)
    @_threadsfor threads for i in 1:length(_res)
        if ismissing(obs_id_left[i]) || ismissing(obs_id_right[i])
            _res[i] = missing
        else
            _res[i] = eq_fun(fl(xl[obs_id_left[i]]), fr(xr[obs_id_right[i]]))
        end
    end
    _res
end
