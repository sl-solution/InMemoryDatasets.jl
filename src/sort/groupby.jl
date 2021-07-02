function groupby!(ds::Dataset, cols::MultiColumnIndex; rev = false, issorted = false)
    if issorted
        sort!(ds, cols, rev = rev, issorted = issorted)
        index(ds).grouped[] = true
        _modified(_attributes(ds))
        ds
    else
        sort!(ds, cols, rev = rev, issorted = issorted)
        index(ds).grouped[] = true
        _modified(_attributes(ds))
        ds
    end
end

groupby!(ds::Dataset, col::ColumnIndex; rev = false, issorted = false) = groupby!(ds, [col]; rev = rev, issorted = issorted)

function ungroup!(ds::Dataset)
    if index(ds).grouped[]
        index(ds).grouped[] = false
        _modified(_attributes(ds))
    end
    ds
end

isgrouped(ds::Dataset)::Bool = index(ds).grouped[]
isgrouped(ds::SubDataset)::Bool = false

function group_starts(ds::Dataset)
    index(ds).starts[1:index(ds).ngroups[]]
end
function getindex_group(ds::Dataset, i::Integer)
    if !(1 <= i <= index(ds).ngroups[])
        throw(BoundsError(ds, i))
    end
    lo = index(ds).starts[i]
    i == index(ds).ngroups[] ? hi = nrow(ds) : hi = index(ds).starts[i+1] - 1
    lo:hi
end
