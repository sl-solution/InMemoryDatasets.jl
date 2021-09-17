function normalize_select(idx, @nospecialize(cols...))
    selected_cols = Int[]
    for i in 1:length(cols)
        normalize_select!(selected_cols, idx, cols[i])
    end
    unique!(selected_cols)
end
function normalize_select!(selected_cols, idx, cols::ColumnIndex)
    push!(selected_cols, idx[cols])
end

function normalize_select!(selected_cols, idx, cols::MultiColumnIndex)
    colsidx = idx[cols]
    for i in 1:length(colsidx)
        push!(selected_cols, colsidx[i])
    end
end

# sell and sell! will replace select and select!
# Dataset shouldn't support copycols since it causes modifying a data set without telling other alias data sets
function select(ds::Dataset, @nospecialize(args...))
    selected_cols = normalize_select(index(ds), args...)
    res = AbstractVector[]

    for j in 1:length(selected_cols)
        push!(res, copy(_columns(ds)[selected_cols[j]]))
    end
    newds = Dataset(res, _names(ds)[selected_cols], copycols = false)
    # else
    #     for j in 1:length(selected_cols)
    #         push!(res, _columns(ds)[selected_cols[j]])
    #     end
    #     newds = Dataset(res, _names(ds)[selected_cols], copycols = false)
    # end
    for j in 1:length(selected_cols)
        setformat!(newds, j => getformat(ds, selected_cols[j]))
    end
    if all(index(ds).sortedcols .∈ Ref(selected_cols))
        scols = Int[]
        revs = copy(index(ds).rev)
        scols_ds = index(ds).sortedcols
        for k in 1:length(scols_ds)
            push!(scols, findfirst(isequal(scols_ds[k]), selected_cols))
        end
        _copy_grouping_info!(newds, ds)
        # need to reorder things
        empty!(index(newds).sortedcols)
        empty!(index(newds).rev)
        append!(index(newds).sortedcols, scols)
        append!(index(newds).rev, revs)
    # else
    #     _reset_grouping_info!(ds)
    end
    setinfo!(newds, _attributes(ds).meta.info[])
    return newds
end

function select!(ds, @nospecialize(args...))
    selected_cols = normalize_select(index(ds), args...)
    unwanted_cols = setdiff(1:ncol(ds), selected_cols)
    sort!(unwanted_cols, rev = true)

    newnames = _names(ds)[selected_cols]
    newlookup = newnames .=> 1:length(selected_cols)
    res = AbstractVector[]

    for j in 1:length(selected_cols)
        push!(res, _columns(ds)[selected_cols[j]])
    end
    newformat = Dict{Int, Function}()
    for j in 1:length(selected_cols)
        push!(newformat, j => getformat(ds, selected_cols[j]))
    end
    if all(index(ds).sortedcols .∈ Ref(selected_cols))
        scols = Int[]
        # revs = copy(index(ds).rev)
        scols_ds = index(ds).sortedcols
        for k in 1:length(scols_ds)
            push!(scols, findfirst(isequal(scols_ds[k]), selected_cols))
        end
        # _copy_grouping_info!(ds, ds)
        # need to reorder things
        empty!(index(ds).sortedcols)
        # empty!(index(ds).rev)
        append!(index(ds).sortedcols, scols)
        # append!(index(ds).rev, revs)
    # else
    #     _reset_grouping_info!(ds)
    end
    for i in unwanted_cols
      popat!(_columns(ds), i)
      delete!(index(ds), i)
    end
    for j in 1:length(selected_cols)
      _columns(ds)[j] = res[j]
    end
    empty!(index(ds).lookup)
    empty!(index(ds).names)
    empty!(index(ds).format)
    for j in newlookup
      push!(index(ds).lookup, j)
    end
    for j in newnames
      push!(index(ds).names, j)
    end
    for j in newformat
      push!(index(ds).format, j)
    end
    _modified(_attributes(ds))
    ds
end
