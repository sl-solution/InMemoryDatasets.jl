##############################################################################
##
## Hcat specialization
##
##############################################################################

# hcat! for 2 arguments, only a vector or a data set is allowed

# Modify Dataset
function hcat!(ds1::Dataset, ds2::AbstractDataset;
               makeunique::Bool=false)
    u = add_names(index(ds1), index(ds2), makeunique=makeunique)
    for i in 1:length(u)
        ds1[!, u[i]] = ds2[:, i]
    end
    for i in 1:length(u)
        setformat!(ds1, u[i]=>getformat(ds2, i))
    end
    _modified(_attributes(ds1))
    return ds1
end

# definition required to avoid hcat! ambiguity

# Modify Dataset
hcat!(ds1::Dataset, ds2::Dataset;
      makeunique::Bool=false) =
    invoke(hcat!, Tuple{Dataset, AbstractDataset}, ds1, ds2,
           makeunique=makeunique)::Dataset

# Modify Dataset
# hcat!(ds::Dataset, x::AbstractVector; makeunique::Bool=false, copycols::Bool=true) =
#     hcat!(ds, Dataset(AbstractVector[x], [:x1], copycols=copycols),
#           makeunique=makeunique, copycols=copycols)

# Modify Dataset
# hcat!(x::AbstractVector, ds::Dataset; makeunique::Bool=false, copycols::Bool=true) =
    # hcat!(Dataset(AbstractVector[x], [:x1], copycols=copycols), ds,
          # makeunique=makeunique, copycols=copycols)

# Modify Dataset
hcat!(x, ds::Dataset; makeunique::Bool=false) =
    throw(ArgumentError("x must be AbstractDataset"))

# Modify Dataset
hcat!(ds::Dataset, x; makeunique::Bool=false) =
    throw(ArgumentError("x must be AbstractVector or AbstractDataset"))

# hcat! for 1-n arguments

# Modify Dataset
hcat!(ds::Dataset; makeunique::Bool=false) = ds

# Modify Dataset
# hcat!(a::Dataset, b, c...; makeunique::Bool=false, copycols::Bool=true) =
#     hcat!(hcat!(a, b, makeunique=makeunique, copycols=copycols),
#           c..., makeunique=makeunique, copycols=copycols)

# hcat

# # Create Dataset
# Base.hcat(ds::Dataset, x; makeunique::Bool=false, copycols::Bool=true) =
#     hcat!(copy(ds, copycols=copycols), x,
#           makeunique=makeunique, copycols=copycols)

# Create Dataset
Base.hcat(ds1::Dataset, ds2::AbstractDataset;
          makeunique::Bool=false) =
    hcat!(copy(ds1), ds2,
          makeunique=makeunique)

# Create Dataset
Base.hcat(ds1::Dataset, ds2::AbstractDataset, dsn::AbstractDataset...;
          makeunique::Bool=false) =
    hcat!(hcat(ds1, ds2, makeunique=makeunique), dsn...,
          makeunique=makeunique)
