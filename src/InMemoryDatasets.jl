module InMemoryDatasets

using Dates
using TableTraits,IteratorInterfaceExtensions
using Reexport
using Compat
using Printf
using PrettyTables, REPL
using Markdown
using PooledArrays
@reexport using Missings, InvertedIndices
import DataAPI,
       DataAPI.All,
       DataAPI.Between,
       DataAPI.Cols,
       DataAPI.describe,
       Tables,
       Tables.columnindex

export
      AbstractDataset,
      DatasetColumns,
      SubDataset,
      Dataset

include("other/utils.jl")
include("other/index.jl")

include("abstractdataset/abstractdataset.jl")
include("dataset/dataset.jl")
include("subdataset/subdataset.jl")
include("datasetrow/datasetrow.jl")

include("other/broadcasting.jl")

include("abstractdataset/iteration.jl")
include("abstractdataset/prettytables.jl")
include("abstractdataset/show.jl")
include("abstractdataset/io.jl")

include("other/tables.jl")


end
