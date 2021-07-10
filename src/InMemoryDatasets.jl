module InMemoryDatasets



# using TableTraits,IteratorInterfaceExtensions
using Reexport
using Compat
using Printf
using PrettyTables, REPL
using Markdown
using PooledArrays
@reexport using Missings, InvertedIndices
@reexport using Statistics
@reexport using Dates
import DataAPI,
       DataAPI.All,
       DataAPI.Between,
       DataAPI.Cols,
       DataAPI.describe,
       DataAPI.innerjoin,
       DataAPI.outerjoin,
       DataAPI.rightjoin,
       DataAPI.leftjoin,
       # DataAPI.semijoin,
       DataAPI.antijoin,
       # DataAPI.crossjoin,
       Tables,
       Tables.columnindex

export
      # types
      AbstractDataset,
      DatasetColumns,
      DatasetColumn,
      SubDataset,
      SubDatasetColumn,
      Dataset,
      GatherBy,
      Between,
      # functions
      nrow,
      ncol,
      getformat,
      setformat!,
      removeformat!,
      content,
      mask,
      groupby!,
      groupby,
      # gatherby,
      ungroup!,
      modify,
      modify!,
      combine,
      setinfo!,
      allowmissing!,
      # from byrow operations
      byrow,
      nunique,
      # from stat
      stdze,
      lag,
      lead,
      rescale,
      wsum,
      wmean,
      topk,
      # from join
      innerjoin,
      outerjoin,
      leftjoin,
      # rightjoin,
      antijoin,
      asofjoin

      




include("other/index.jl")
include("other/utils.jl")
include("stat/non_hp_stat.jl")
include("stat/hp_stat.jl")
include("stat/stat.jl")
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
# ds stat
include("stat/ds_stat.jl")
# other functions
include("dataset/other.jl")
include("subdataset/subdataset.jl")
include("datasetrow/datasetrow.jl")
include("other/broadcasting.jl")

# modifying dataset
include("dataset/modify.jl")
include("dataset/combine.jl")
include("abstractdataset/selection.jl")
# sorting
include("sort/util.jl")
include("sort/qsort.jl")
include("sort/int.jl")
include("sort/pooled.jl")
include("sort/sortperm.jl")
include("sort/sort.jl")
include("sort/groupby.jl")
include("sort/gatherby.jl")

# joins
include("join/join.jl")
include("join/asof.jl")
include("join/main.jl")

include("abstractdataset/iteration.jl")
include("abstractdataset/prettytables.jl")
include("abstractdataset/show.jl")
include("datasetrow/show.jl")

include("abstractdataset/io.jl")

include("other/tables.jl")

end
