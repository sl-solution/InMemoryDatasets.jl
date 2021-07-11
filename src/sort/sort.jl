Base.sortperm(ds::Dataset, cols; rev = false) = _sortperm(ds, cols, rev)[2]

function Base.sort!(ds::Dataset, cols::MultiColumnIndex; rev = false, issorted = false)
    colsidx = index(ds)[cols]
    if length(rev) == 1
        revs = repeat([rev], length(colsidx))
    else
        revs = rev
    end

    @assert length(colsidx) == length(revs) "the reverse argument must be the same length as the length of selected columns"
    if issorted
        index(ds).sortedcols == colsidx && index(ds).rev == revs && return ds
        selected_columns, ranges, last_valid_index = _find_starts_of_groups(ds::Dataset, colsidx, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
        _reset_grouping_info!(ds)
        append!(index(ds).sortedcols, selected_columns)
        append!(index(ds).rev, revs)
        append!(index(ds).perm, collect(1:nrow(ds)))
        append!(index(ds).starts, ranges)
        index(ds).ngroups[] = last_valid_index
        _modified(_attributes(ds))
        ds
    else
        starts, perm, ngroups = _sortperm(ds, cols, revs)
        _reset_grouping_info!(ds)
        append!(index(ds).sortedcols, collect(colsidx))
        append!(index(ds).rev, revs)
        append!(index(ds).perm, perm)
        append!(index(ds).starts, starts)
        index(ds).ngroups[] = ngroups
        _modified(_attributes(ds))
        _permute_ds_after_sort!(ds, perm)
    end
    ds
end


Base.sort!(ds::Dataset, col::ColumnIndex; rev::Bool = false, issorted = false) = sort!(ds, [col], rev = rev, issorted = issorted)
