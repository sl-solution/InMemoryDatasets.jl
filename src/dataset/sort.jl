function Base.sort(ds::Dataset, cols::MultiColumnIndex; issorted = false)
    colsidx = index(ds)[cols]
    if issorted
        selected_columns, ranges, last_valid_index = _find_starts_of_groups(ds::Dataset, colsidx, nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64))
        sds = SortedDataset(copy(ds), selected_columns, ranges, last_valid_index)
        _modified(_attributes(sds))
        sds
    else
        @error "not yet implemented for `issorted = false`"
    end
end
