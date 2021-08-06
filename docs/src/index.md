# InMemoryDatasets.jl

Welcome to the InMemoryDatasets.jl documentation!

This resource aims to teach you everything you need to know to get up and
running with tabular data manipulation using the InMemoryDatasets.jl package.

## What are In Memory Datasets?

InMemoryDatasets.jl provides a set of tools for working with tabular data in Julia.
Its design and functionality are similar to those of `DataFrames.jl` in Julia, `data manipulation` (in Base/SAS),
`data manipulation` (in stata), `polars` (in rust), `pandas` (in Python), and `data.table` (in R),
making it a great general purpose data science tool.

InMemoryDatasets.jl isn't the only tool for working with tabular data in Julia -- as noted below,
there are some other great libraries for certain use-cases -- but it provides great data
manipulations functionality through a flexible interface.

## InMemoryDatasets.jl and the Julia Data Ecosystem

The Julia data ecosystem can be a difficult space for new users to navigate, in
part because the Julia ecosystem tends to distribute functionality across
different libraries more than some other languages. Because many people coming
to InMemoryDatasets.jl are just starting to explore the Julia data ecosystem, below is
a list of well-supported libraries that provide different data science tools,
along with a few notes about what makes each library special.

- **Statistics**
    - [FreqTables.jl](https://github.com/nalimilan/FreqTables.jl): Create
      frequency tables / cross-tabulations. Tightly integrated with InMemoryDatasets.jl.
    - [GLM.jl](https://juliastats.org/GLM.jl/stable/manual/): Tools for estimating
      linear and generalised linear models.
- **Plotting**
    - [Plots.jl](http://docs.juliaplots.org/latest/): Powerful, modern plotting
      library with a syntax.
      [StatsPlots.jl](http://docs.juliaplots.org/latest/tutorial/#Using-Plot-Recipes-1)
      provides Plots.jl with recipes for many standard statistical plots.
    - [VegaLite.jl](https://www.queryverse.org/VegaLite.jl/stable/): High-level
      plotting library that uses a different "grammar of graphics" syntax.

### Other Julia Tabular Libraries

In memory Datasets are great general purpose tool for data manipulation and
wrangling. However, the following packages in Julia provide different approach for
working with tabular data:

- [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl): A Julia package for
  working with tabular data. With many similarities to Datasets.
- [TypedTables.jl](https://juliadata.github.io/TypedTables.jl/stable/):
  Type-stable heterogeneous tables. Useful for improved performance when the
  structure of your table is relatively stable and does not feature thousands of
  columns.
- [JuliaDB.jl](https://juliadata.github.io/JuliaDB.jl/stable/): For users
  working with data that is too large to fit in memory.

Note that most tabular data libraries in the Julia ecosystem (including
InMemoryDatasets.jl) support a common interface (defined in the
[Tables.jl](https://github.com/JuliaData/Tables.jl) package). As a result, some
libraries are capable or working with a range of tabular data structures, making
it easy to move between tabular libraries as your needs change.

## Questions?

If there is something you expect InMemoryDatasets to be capable of, but
cannot figure out how to do, please reach out with questions in Domains/Data on
[Discourse](https://discourse.julialang.org/new-topic?title=[InMemoryDatasets%20Question]:%20&body=%23%20Question:%0A%0A%23%20Dataset%20(if%20applicable):%0A%0A%23%20Minimal%20Working%20Example%20(if%20applicable):%0A&category=Domains/Data&tags=question).

Please report bugs by
[opening an issue](https://github.com/sl-solution/InMemoryDatasets.jl/issues/new).

You can follow the **source** links throughout the documentation to jump right
to the source files on GitHub to make pull requests for improving the
documentation and function capabilities.

Information on specific versions can be found on the [Release
page](https://github.com/sl-solution/InMemoryDatasets.jl/releases).

## Package Manual

```@contents
Pages = ["man/basics.md",
         "man/tutorial.md",
         "man/formats.jl",
         "man/map.md",
         "man/byrow.md",
         "man/filter.md",
         "man/sorting.md",
         "man/grouping.md",
         "man/aggregation",
         "man/transpose.md",
         "man/joins.md"]
Depth = 2
```

## API

Only exported (i.e. available for use without `InMemoryDatasets.` qualifier after
loading the InMemoryDatasets.jl package with `using InMemoryDatasets`) types and functions
are considered a part of the public API of the InMemoryDatasets.jl package. In general
all such objects are documented in this manual (in case some documentation is
missing please kindly report an issue
[here](https://github.com/sl-solution/InMemoryDatasets.jl/issues/new)).

Please be warned that while Julia allows you to access internal functions or
types of InMemoryDatasets.jl these can change without warning between versions of
InMemoryDatasets.jl. In particular it is not safe to directly access fields of types
that are a part of public API of the InMemoryDatasets.jl package using e.g. the
`getfield` function. Whenever some operation on fields of defined types is
considered allowed an appropriate exported function should be used instead.

<!-- ```@contents
Pages = ["lib/functions.md"]
Depth = 2
```

## Index

```@index
Pages = ["lib/functions.md"]
``` -->
