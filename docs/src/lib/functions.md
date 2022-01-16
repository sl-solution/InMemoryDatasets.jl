```@meta
CurrentModule = InMemoryDatasets
```

# Functions

```@index
Pages = ["functions.md"]
```

## Constructing data set
```@docs
copy
repeat
repeat!
similar
```

## Formats
```@docs
removeformat!
setformat!
```

## Summary information
```@docs
content
getinfo
ncol
ndims
nrow
setinfo!
size
```

## Working with column names
```@docs
names
propertynames
rename
rename!
```

## Modifying data sets
```@docs
append!
combine
flatten
hcat
insertcols!
map
map!
mapcols
modify
modify!
push!
repeat
repeat!
select
select!
update
update!
```

## Transposing and reshaping data sets
```@docs
flatten
flatten!
transpose
```

<!-- ## Sorting
```@docs
issorted
issorted!
sort
sort!
sortperm
``` -->

## Joining
```@docs
antijoin
antijoin!
closejoin
closejoin!
innerjoin
leftjoin
leftjoin!
outerjoin
semijoin
semijoin!
update
update!
```

<!-- ## Grouping
```@docs
groupby
groupby!
ungroup!
``` -->

## Filtering rows
```@docs
byrow
byrow(all)
byrow(any)
byrow(count)
byrow(in)
byrow(isequal)
byrow(isless)
byrow(mean)
byrow(prod)
byrow(sum)
compare
contains
deleteat!
duplicates
empty
empty!
first
filter
filter!
last
mask
unique
unique!
```

## Working with missing values
```@docs
byrow
completecases
dropmissing
dropmissing!
map
map!
```

## Statistics
```@docs
lag
lag!
lead
lead!
rescale
stdze
topk
```
