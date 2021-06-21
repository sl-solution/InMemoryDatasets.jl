function groupby!(ds::Dataset, cols::MultiColumnIndex, rev::Vector{<:Bool}; issorted = false)
    if issorted
        sort!(ds, cols, rev, issorted = issorted)
        index(ds).grouped[] = true
        _modified(_attributes(ds))
        ds
    else
        @error "not yet implemented"
    end
end

function groupby!(ds::Dataset, cols::MultiColumnIndex, rev::Bool; issorted = false)
    colsidx = index(ds)[cols]
    groupby!(ds, cols, repeat([rev], length(colsidx)), issorted = issorted)
end


groupby!(ds::Dataset, cols::MultiColumnIndex; issorted = false) = groupby!(ds, cols, false, issorted = issorted)
groupby!(ds::Dataset, col::ColumnIndex; issorted = false) = groupby!(ds, [col], issorted = issorted)
groupby!(ds::Dataset, col::ColumnIndex, rev::Bool; issorted = false) = groupby!(ds, [col], rev,  issorted = issorted)

function ungroup!(ds::Dataset)
    if index(ds).grouped[]
        index(ds).grouped[] = false
        _modified(_attributes(ds))
    end
    ds
end

isgrouped(ds::Dataset) = index(ds).grouped[]
isgrouped(ds::SubDataset) = false
