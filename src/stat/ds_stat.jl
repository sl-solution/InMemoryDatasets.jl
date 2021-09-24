function _sum_ds_f_barrier!(res, ngroups, starts, x, ds, by, i)
    for j in 1:ngroups
        lo = starts[j]
        j == ngroups ? hi = nrow(ds) : hi = starts[j + 1] - 1
        res[(i-1)*ngroups+j] = hp_sum(by, view(x, lo:hi))
    end
end

# Not ok yet, don't use it
function Base.sum(ds::Dataset, cols::MultiColumnIndex; by = identity, threads = true)
    colsidx = index(ds)[cols]
    newds = Dataset()
    if isgrouped(ds)
        ngroups = index(ds).ngroups[]
        starts = index(ds).starts
        groupcols = index(ds).sortedcols
        for j in 1:length(groupcols)
            insertcols!(newds, _names(ds)[groupcols[j]]=>repeat(_columns(ds)[groupcols[j]][view(starts,1:ngroups)], length(colsidx)))
        end
        insertcols!(newds, :var=>repeat(_names(ds)[colsidx], inner = ngroups))
        res = Tables.allocatecolumn(Real, nrow(newds))
        for i in 1:length(colsidx)
            _sum_ds_f_barrier!(res, ngroups, starts, _columns(ds)[colsidx[i]], ds, by, i)
        end
        insertcols!(newds, ncol(newds)+1, :sum => res, unsupported_copy_cols = false)

    else
        insertcols!(newds, :var=>_names(ds)[colsidx])
        if threads
            insertcols!(newds, :sum => [hp_sum(by, _columns(ds)[colsidx[j]]) for j in 1:length(colsidx)])
        else
            insertcols!(newds, :sum => [sum(by, _columns(ds)[colsidx[j]]) for j in 1:length(colsidx)])
        end
    end
    newds
end
Base.sum(ds::Dataset, col::ColumnIndex; by = identity, threads = true) = sum(ds, [col], by = by, threads = threads)
