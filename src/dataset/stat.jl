
function Base.sum(f, ds::AbstractDataset; dims = 1)
    if dims == 1
        [sum(f, _columns(ds)[i]) for i in 1:ncol(ds)]
    elseif dims == 2
        byrow(ds, sum, :, by = f)
    else
        throw(ArgumentError("the dims can only be 1 (column wise) or 2 (row wise)"))
    end
end
Base.sum(ds::AbstractDataset; dims = 1) = sum(identity, ds; dims = dims)
