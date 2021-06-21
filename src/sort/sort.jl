function Base.sort!(ds::Dataset, cols::MultiColumnIndex, rev::Vector{<:Bool}; issorted = false)
    colsidx = index(ds)[cols]
    @assert length(colsidx) == length(rev) "the reverse arugment must be the same length as the length of selected columns"
    if issorted
        index(ds).sortedcols == colsidx && index(ds).rev == rev && return ds
        selected_columns, ranges, last_valid_index = _find_starts_of_groups(ds::Dataset, colsidx, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
        _reset_grouping_info!(ds)
        append!(index(ds).sortedcols, selected_columns)
        append!(index(ds).rev, rev)
        append!(index(ds).perm, collect(1:nrow(ds)))
        append!(index(ds).starts, ranges)
        index(ds).ngroups[] = last_valid_index
        _modified(_attributes(ds))
        ds
    else
        @error "not yet implemented for `issorted = false`"
    end
end

function Base.sort!(ds::Dataset, cols::MultiColumnIndex, rev::Bool; issorted = false)
    colsidx = index(ds)[cols]
    sort!(ds, cols, repeat([rev], length(colsidx)), issorted = issorted)
end

Base.sort!(ds::Dataset, cols::MultiColumnIndex; issorted = false) = sort!(ds, cols, false, issorted = issorted)
Base.sort!(ds::Dataset, col::ColumnIndex; issorted = false) = sort!(ds, [col], issorted = issorted)
Base.sort!(ds::Dataset, col::ColumnIndex, rev::Bool; issorted = false) = sort!(ds, [col], rev, issorted = issorted)
