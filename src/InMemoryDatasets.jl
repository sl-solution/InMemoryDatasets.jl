module InMemoryDatasets

using Dates
# using TableTraits,IteratorInterfaceExtensions
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
      DatasetColumn,
      SubDataset,
      Dataset,
      SortedDataset,
      nrow,
      ncol,
      getformat,
      setformat!,
      removeformat!,
      content

include("other/index.jl")
include("other/utils.jl")

include("abstractdataset/abstractdataset.jl")
# create dataset
include("dataset/constructor.jl")
# get elements
include("dataset/getindex.jl")
# set elements
include("dataset/setindex.jl")
# delete and append observations
include("dataset/del_and_append.jl")
# concatenate
include("dataset/cat.jl")
# other functions
include("dataset/other.jl")
include("subdataset/subdataset.jl")
include("sorteddataset/sorteddataset.jl")
include("datasetrow/datasetrow.jl")
# TODO needs correction for formats and other metadata
include("other/broadcasting.jl")

include("abstractdataset/selection.jl")
include("dataset/sort.jl")

include("abstractdataset/iteration.jl")
include("abstractdataset/prettytables.jl")
include("abstractdataset/show.jl")
include("sorteddataset/show.jl")
include("abstractdataset/io.jl")

include("other/tables.jl")


end
