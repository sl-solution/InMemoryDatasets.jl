using Documenter
using InMemoryDatasets

# DocMeta.setdocmeta!(InMemoryDatasets, :DocTestSetup, :(using InMemoryDatasets); recursive=true)

# Build documentation.
# ====================

makedocs(
    # options
    # modules = [InMemoryDatasets],
    doctest = false,
    clean = false,
    sitename = "In Memory Datasets",
    # format = Documenter.HTML(
    #     canonical = "https://sl-solution.github.io/InMemoryDataset.jl/stable/",
    #     edit_link = "main"
    # ),
    pages = Any[
        "Introduction" => "index.md",
        "First Steps" => "man/basics.md",
        "User Guide" => Any[
            "Tutorial" => "man/tutorial.md",
            "Formats" => "man/formats.md",
            "Call functions on each observation" => "man/map.md",
            "Row-wise operations" => "man/byrow.md",
            "Transform columns" => "man/modify.md",
            "Filter observations" => "man/filter.md",
            "Sort" => "man/sorting.md",
            "Group observations" => "man/grouping.md",
            "Aggregation" => "man/aggregation.md",
            "Transpose data" => "man/transpose.md",
            "Joins" => "man/joins.md"
        ],
        "Gallery" => "man/gallery.md"
        # "API" => Any[
        #     "Functions" => "lib/functions.md"
        # ]
    ],
    strict = true
)

# Deploy built documentation from Travis.
# =======================================

deploydocs(
    # options
    repo = "github.com/sl-solution/InMemoryDatasets.jl",
    target = "build",
    deps = nothing,
    make = nothing,
    devbranch = "main"
)
