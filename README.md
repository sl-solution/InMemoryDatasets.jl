# Datasets

`InMemoryDatasets.jl` is a `Julia` package for working with tabular data sets.

# Examples

```julia
> using InMemoryDatasets
> a = Dataset(x = [1,2], y = [1,2])
2×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64     Int64
─────┼────────────────────
   1 │        1         1
   2 │        2         2
