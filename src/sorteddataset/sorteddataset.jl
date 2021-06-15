# this is the first attemp, and it seems that this design is not working for our purpose
# should change our approach

struct SortedDataset{T<:AbstractDataset}
    parent::T
    cols::Vector{Int}
    starts::Vector
    ngroups::Int
end

Base.parent(sds::SortedDataset) = getfield(sds, :parent)
index(sds::SortedDataset) = index(parent(sds))
_attributes(sds::SortedDataset) = _attributes(parent(sds))
_columns(sds::SortedDataset) = _columns(parent(sds))
Base.size(sds::SortedDataset) =  size(parent(sds))
Base.names(sds::SortedDataset) = names(parent(sds))
Base.names(sds::SortedDataset, cols) = names(parent(sds), cols)
_names(sds::SortedDataset) = _names(parent(sds))
sortedcol(sds::SortedDataset) = names(sds, getfield(sds, :cols))
ngroups(sds::SortedDataset) = getfield(sds, :ngroups)
_starts_of_groups(sds::SortedDataset) = getfield(sds, :starts)


function Base.:(==)(sds1::SortedDataset, sds2::SortedDataset)
    sortedcol(sds1) == sortedcol(sds2) &&
    # this is just for fast comparison, o.w. comparing parents are enough
        ngroups(sds1) == ngroups(sds2) &&
            all(_starts_of_groups(sds1)[i] == _starts_of_groups(sds2)[i] for i in 1:ngroups(sds1)) &&
                parent(sds1) == parent(sds2)
end

function Base.isequal(sds1::SortedDataset, sds2::SortedDataset)
    isequal(sortedcol(sds1), sortedcol(sds2)) &&
    # this is just for fast comparison, o.w. comparing parents are enough
        isequal(ngroups(sds1), ngroups(sds2)) &&
            all(isequal(_starts_of_groups(sds1)[i], _starts_of_groups(sds2)[i]) for i in 1:ngroups(sds1)) &&
                isequal(parent(sds1), parent(sds2))
end
