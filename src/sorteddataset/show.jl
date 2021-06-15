function Base.summary(io::IO, sds::SortedDataset)
    N = getfield(sds, :ngroups)
    keystr = length(sortedcol(sds)) > 1 ? "keys" : "key"
    groupstr = N == 1 ? "group" : "groups"
    print(io, "SortedDataset with $N $groupstr based on $keystr: ")
    join(io, sortedcol(sds), ", ")
end

function Base.show(io::IO, sds::SortedDataset; kwargs...)
    println(io, summary(sds))
    show(io, parent(sds), kwargs...)
end
