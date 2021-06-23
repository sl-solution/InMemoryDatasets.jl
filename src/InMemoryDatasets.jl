module InMemoryDatasets

using Dates
using Statistics
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
      SubDatasetColumn,
      Dataset,
      nrow,
      ncol,
      getformat,
      setformat!,
      removeformat!,
      content,
      mask,
      groupby!,
      ungroup!,
      modify,
      modify!,
      byrow,
      setinfo!

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

# byrow operations
include("byrow/row_functions.jl")
include("byrow/hp_row_functions.jl")
include("byrow/byrow.jl")
# other functions
include("dataset/other.jl")
include("subdataset/subdataset.jl")
include("datasetrow/datasetrow.jl")
include("other/broadcasting.jl")

# modifying dataset
include("dataset/modify.jl")
include("abstractdataset/selection.jl")
include("sort/sort.jl")
include("sort/groupby.jl")

include("abstractdataset/iteration.jl")
include("abstractdataset/prettytables.jl")
include("abstractdataset/show.jl")
include("datasetrow/show.jl")

include("abstractdataset/io.jl")

include("other/tables.jl")


end
