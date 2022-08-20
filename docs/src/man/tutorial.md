# Tutorial

An extended version of this tutorial is available [here](https://github.com/sl-solution/InMemoryDatasetsTutorial/blob/main/flights.ipynb).

## Getting and loading the data

We recommend the `DLMReader` package to load a csv file into `InMemoryDatasets`, `DLMReader` is a high-performance and versatile package for reading delimited files into `InMemoryDatasets` (there are many alternative packages in Julia ecosystem, users can use any of them to read data into `InMemoryDatasets`):


```julia
julia> using InMemoryDatasets

julia> using DLMReader

julia> using PooledArrays

julia> import Downloads

julia> _tmp = Downloads.download("https://raw.githubusercontent.com/sl-solution/InMemoryDatasetsTutorial/main/CA_flights.csv");

julia> flights = filereader(_tmp, dtformat = Dict(1=>dateformat"y-m-d"));

julia> modify!(flights, [:IATA, :Tail_Number, :Origin, :Dest, :CancellationCode] => PooledArray) # convert Strings to PooledArray for efficiency - We discuss the modify! function later

julia> setinfo!(flights, "Reporting Carrier On-Time Performance for all flights in 2020 from CA. Downloaded from www.transtats.bts.gov");
```

## Filtering the data

In order to select only rows matching certain criteria, use the `filter` function. Note that for referring to a single column in a data set, we can use the column name (a Symbol), the column name as String, or the column index


```julia
julia> filter(flights, :FlightDate, by = x->monthday(x)==(1,1))

julia> filter(flights, 1, by = x->monthday(x)==(1,1), view = true) # creating a view of filtered observations
```

To test if one of two conditions is verified:


```julia
julia> filter(flights, :IATA, by = in(("AA", "UA")))
```

## Select: pick columns by name

You can use the `select!`/`select` functions to select a subset of columns, unlike `select!`, `select` makes a copy of data


```julia
julia> select(flights, :DepTime, :ArrTime, :Flight_Number)
467402×3 Dataset
    Row │ DepTime   ArrTime   Flight_Number
        │ identity  identity  identity      
        │ Int64?    Int64?    Int64?        
────────┼───────────────────────────────────
      1 │     2354       820            988
      2 │     2345       757            988
      3 │     2350       825            988
      4 │     2400       754            988
      5 │     2355       757            988
      6 │     2357       815            988
      7 │     2350       759            988
      8 │     2345       759            988
      9 │     2149       604            988
     10 │     2152       606            988
   ⋮    │    ⋮         ⋮            ⋮
 467393 │     1247      1538            326
 467394 │     2150      2253             99
 467395 │     1421      1549             67
 467396 │     1237      1527            326
 467397 │     1833      2017            598
 467398 │     1326      1505            582
 467399 │      657      1255            317
 467400 │      706      1237            317
 467401 │      842      1348            318
 467402 │     1651      1759             25
                         467382 rows omitted
```

`InMemoryDatasets` provides different ways for referring to multiple columns. For instance, let's select all columns between `:FlightDate` and `:Dest` as well as all columns containing "Taxi" or "Delay" in their names. `Between` selects columns between two specified extremes, and regular expressions can be used to select columns with specified patterns.


```julia
julia> select(flights, Between(:FlightDate, :Dest), r"Taxi|Delay")
467402×10 Dataset
    Row │ FlightDate  IATA      Tail_Number  Flight_Number  Origin    Dest      TaxiOut   TaxiIn    DepDelay  ArrDelay
        │ identity    identity  identity     identity       identity  identity  identity  identity  identity  identity
        │ Date?       String?   String?      Int64?         String?   String?   Float64?  Float64?  Float64?  Float64?
────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────
      1 │ 2020-01-01  B6        N947JB                 988  LAX       BOS           26.0      14.0      -5.0      -2.0
      2 │ 2020-01-02  B6        N987JT                 988  LAX       BOS           16.0       9.0     -14.0     -25.0
      3 │ 2020-01-03  B6        N986JB                 988  LAX       BOS           24.0      17.0      -9.0       3.0
      4 │ 2020-01-04  B6        N964JT                 988  LAX       BOS           16.0       1.0       1.0     -28.0
      5 │ 2020-01-05  B6        N981JT                 988  LAX       BOS           22.0       8.0      -4.0     -25.0
      6 │ 2020-01-06  B6        N961JT                 988  LAX       BOS           22.0      11.0      -2.0      -7.0
      7 │ 2020-01-07  B6        N935JB                 988  LAX       BOS           20.0       2.0      -9.0     -23.0
      8 │ 2020-01-08  B6        N947JB                 988  LAX       BOS           11.0       8.0     -14.0     -23.0
      9 │ 2020-01-09  B6        N980JT                 988  LAX       BOS           13.0      15.0      -6.0     -13.0
     10 │ 2020-01-10  B6        N968JT                 988  LAX       BOS           17.0       6.0      -3.0     -11.0
   ⋮    │     ⋮          ⋮           ⋮             ⋮           ⋮         ⋮         ⋮         ⋮         ⋮         ⋮
 467393 │ 2020-09-27  G4        314NV                  326  LAX       BOI            9.0       7.0      -8.0     -17.0
 467394 │ 2020-09-25  G4        272NV                   99  SMX       LAS            6.0       7.0     -13.0     -22.0
 467395 │ 2020-09-06  G4        307NV                   67  OAK       LAS           10.0       9.0      -1.0      -1.0
 467396 │ 2020-09-06  G4        337NV                  326  LAX       BOI           11.0       4.0     -14.0     -24.0
 467397 │ 2020-09-07  G4        247NV                  598  SCK       AZA           10.0       7.0      13.0      15.0
 467398 │ 2020-09-20  G4        256NV                  582  SCK       AZA            5.0       8.0      -2.0      -5.0
 467399 │ 2020-09-07  G4        318NV                  317  LAX       MEM           14.0      24.0      -3.0      12.0
 467400 │ 2020-09-14  G4        318NV                  317  LAX       MEM            8.0      12.0       6.0      -9.0
 467401 │ 2020-09-26  G4        328NV                  318  LAX       SGF           10.0       4.0     -18.0     -26.0
 467402 │ 2020-09-11  G4        312NV                   25  FAT       LAS           11.0       4.0       6.0       6.0
                                                                                                    467382 rows omitted
```

## Applying several operations

There are several packages in `Julia` to apply several operations one after the other, here we demonstrate the `Chain` package.

Let's assume we want to select `:IATA` and `:DepDelay` columns and filter for delays over 60 minutes.  Here we assume we want a copy of the result (compared to view of the result). The `@chain` macro automatically fills the first argument of the chained functions, thus, `flights` becomes the first argument `view`, and the output of `view` becomes the first argument of `filter`. We passed `missings = false` to `filter` to drop those rows in `flights` where `:DepDelay` is missing.

```julia
julia> using Chain

julia> delayed =  @chain flights begin
                     view(:, [:IATA, :DepDelay])
                     filter(:DepDelay, by = >(60), missings = false)
                   end
10614×2 Dataset
   Row │ IATA      DepDelay
       │ identity  identity
       │ String?   Float64?
───────┼────────────────────
     1 │ B6           240.0
     2 │ B6           164.0
     3 │ B6            61.0
     4 │ B6            93.0
     5 │ B6            93.0
     6 │ B6            92.0
     7 │ B6            80.0
     8 │ B6           101.0
     9 │ B6            99.0
    10 │ B6            70.0
   ⋮   │    ⋮         ⋮
 10605 │ OO            87.0
 10606 │ OO           132.0
 10607 │ OO           129.0
 10608 │ OO            70.0
 10609 │ OO            93.0
 10610 │ OO            71.0
 10611 │ OO           118.0
 10612 │ OO           153.0
 10613 │ OO           177.0
 10614 │ OO            96.0
          10594 rows omitted
```

## Reorder rows

Select `:IATA` and `:DepDelay` columns while sorted by `:DepDelay`. Note that in the following code, `flights[!, [:IATA, :DepDelay]]` is equivalent to `view(flights, :, [:IATA, :DepDelay])`. Further notice that, by default, calling `sort` on a view of a data set creates a new data set, however, passing `view = true` creates a view of sorted values.

> Note `sort!` and `sort` reorder observations instead of sorting observations by reference (i.e. view of sorted data), however, we can use `groupby`, the combination of `sortperm` and `view`, or pass a view of a data set and set `view = true` to create a sorting data set by reference.


```julia
julia> sort(flights[!, [:IATA, :DepDelay]], 2)
467402×2 Sorted Dataset
 Sorted by: DepDelay
    Row │ IATA      DepDelay  
        │ identity  identity  
        │ String?   Float64?  
────────┼─────────────────────
      1 │ OO            -66.0
      2 │ OO            -61.0
      3 │ G4            -49.0
      4 │ OO            -47.0
      5 │ WN            -40.0
      6 │ OO            -40.0
      7 │ B6            -40.0
      8 │ UA            -35.0
      9 │ UA            -35.0
     10 │ B6            -35.0
   ⋮    │    ⋮          ⋮
 467394 │ OO        missing   
 467395 │ OO        missing   
 467396 │ OO        missing   
 467397 │ OO        missing   
 467398 │ OO        missing   
 467399 │ G4        missing   
 467400 │ G4        missing   
 467401 │ G4        missing   
 467402 │ G4        missing   
           467383 rows omitted
```

or, in reverse order:

```julia
julia> sort(flights[!, [:IATA, :DepDelay]], 2, rev = true)
```

## Add new columns

Use the `modify!` or `modify` (`modify` makes a copy of data) functions to add a column to an existing dataset. In the following example we calculate speed, by dividing distance by air time. Here, we multiply `:Speed` by 60 since `:AirTime` is in minutes - it is also recommended to define functions outside `modify!`, since they will be reusable and the second time you call them no compilation is needed.

```julia
julia> m2h(x) = 60x
julia> modify!(flights, [:Distance, :AirTime] => byrow(/) => :Speed, :Speed => byrow(m2h))
```

## Applying functions on each group of observations

To get the average delay for each destination, we `groupby` our data set by `:Dest` (if the order of original data set should be preserved, we must use `gatherby`), select `:ArrDelay` and compute the mean:


```julia
julia> combine(groupby(flights, :Dest), :ArrDelay => IMD.mean)
120×2 Dataset
 Row │ Dest      mean_ArrDelay
     │ identity  identity      
     │ String?   Float64?      
─────┼─────────────────────────
   1 │ ABQ           -6.77667
   2 │ ACV           -3.12598
   3 │ ANC           -2.45455
   4 │ ASE           14.446
   5 │ ATL           -4.97706
   6 │ AUS           -6.76768
   7 │ AZA           16.3775
   8 │ BDL           -2.16667
   9 │ BFL           -2.35662
  10 │ BIL           -7.04651
  ⋮  │    ⋮            ⋮
 111 │ SLC           -2.27623
 112 │ SMF           -9.57659
 113 │ SNA           -8.61454
 114 │ STL           -7.94696
 115 │ STS           -4.18345
 116 │ SUN           -5.41912
 117 │ TPA          -11.1995
 118 │ TUL           -2.94444
 119 │ TUS           -5.57618
 120 │ XNA          -13.3143
               100 rows omitted
```

we can summarise several columns at the same time, e.g. for each carrier, calculate the minimum and maximum arrival and departure delays:(Note that in the following code, `r"Delay" => [IMD.minimum, IMD.maximum]` is normalised as `names(flights, r"Delay") .=> Ref([IMD.minimum, IMD.maximum])`)


```julia
julia> @chain flights begin
           groupby(:IATA)
           combine(r"Delay" => [IMD.minimum, IMD.maximum])
        end
14×5 Dataset
 Row │ IATA      minimum_DepDelay  maximum_DepDelay  minimum_ArrDelay  maximum_ArrDelay
     │ identity  identity          identity          identity          identity         
     │ String?   Float64?          Float64?          Float64?          Float64?         
─────┼──────────────────────────────────────────────────────────────────────────────────
   1 │ AA                   -29.0            2466.0             -75.0            2457.0
   2 │ AS                   -34.0             590.0             -92.0             603.0
   3 │ B6                   -40.0            1076.0             -91.0            1074.0
   4 │ DL                   -28.0            1154.0             -75.0            1157.0
   5 │ F9                   -34.0             372.0             -69.0             353.0
   6 │ G4                   -49.0            1516.0             -60.0            1511.0
   7 │ HA                   -22.0             659.0             -80.0             620.0
   8 │ MQ                   -22.0            1223.0             -48.0            1223.0
   9 │ NK                   -34.0            1339.0             -63.0            1160.0
  10 │ OO                   -66.0            1531.0             -77.0            1528.0
  11 │ UA                   -35.0            1182.0             -80.0            1185.0
  12 │ WN                   -40.0             421.0             -84.0             415.0
  13 │ YV                   -30.0            1170.0             -74.0            1165.0
  14 │ YX                   -13.0             147.0             -22.0             136.0
```

For each day of the year, count the total number of flights and sort in descending order:


```julia
julia> @chain flights begin
           setformat!(1 => day) # format date as day
           gatherby(1)
           combine(1 => length => :count)
           sort!(2, rev = true)
        end
31×2 Sorted Dataset
 Sorted by: count
 Row │ FlightDate  count    
     │ day         identity
     │ Date?       Int64?   
─────┼──────────────────────
   1 │ 3              15981
   2 │ 6              15975
   3 │ 2              15845
   4 │ 13             15827
   5 │ 20             15802
   6 │ 23             15757
   7 │ 27             15727
   8 │ 10             15672
   9 │ 9              15670
  10 │ 5              15661
  ⋮  │     ⋮          ⋮
  23 │ 8              15015
  24 │ 29             15007
  25 │ 14             14776
  26 │ 18             14771
  27 │ 11             14749
  28 │ 25             14693
  29 │ 15             14591
  30 │ 30             13455
  31 │ 31              9071
             12 rows omitted
```
For each month of the year, calculate the cancellation rate:

```julia
julia> pct_fmt(x) = string(round(x*100, digits = 2), "%") # we use this as format for displaying values

julia> @chain flights begin
           setformat!(1 => month)
           groupby(:FlightDate)
           combine(:Cancelled => mean => :Percent)
           setformat!(:Percent => pct_fmt)
       end
12×2 Dataset
 Row │ FlightDate  Percent  
     │ month       pct_fmt  
     │ Date?       Float64?
─────┼──────────────────────
   1 │ 1              0.94%
   2 │ 2              0.75%
   3 │ 3             17.58%
   4 │ 4             44.05%
   5 │ 5              5.21%
   6 │ 6              0.27%
   7 │ 7              0.63%
   8 │ 8              0.68%
   9 │ 9              0.47%
  10 │ 10             0.21%
  11 │ 11             0.35%
  12 │ 12             0.77%
```

For each destination, count the total number of flights and the number of distinct planes that flew there


```julia
julia> @chain flights begin
          groupby(:Dest)
          combine(:Tail_Number .=> [length, length∘union] .=> [:Count, :Unique_Flight])
        end
120×3 Dataset
 Row │ Dest      Count     Unique_Flight
     │ identity  identity  identity      
     │ String?   Int64?    Int64?        
─────┼───────────────────────────────────
   1 │ ABQ           1861            684
   2 │ ACV           1097            202
   3 │ ANC            135             67
   4 │ ASE            823             77
   5 │ ATL           9604           1323
   6 │ AUS           4678           1527
   7 │ AZA            232             42
   8 │ BDL             13             12
   9 │ BFL            297            137
  10 │ BIL             44             16
  ⋮  │    ⋮         ⋮            ⋮
 111 │ SLC          19590           1755
 112 │ SMF          13750           1186
 113 │ SNA           7908           1077
 114 │ STL           2371            777
 115 │ STS           1025            122
 116 │ SUN            295             97
 117 │ TPA            401            254
 118 │ TUL             83             14
 119 │ TUS           2272            743
 120 │ XNA            106             57
                         100 rows omitted
```

## Non-reduction functions

In the previous section, we always applied functions that reduced a vector to a single value.
Non-reduction functions instead take a vector and return a vector. For example we can rank, within each `:IATA`, how much
delay a given flight had and figure out the day and month with the two greatest delays: (Note that for using a multivariate function in `combine`, the input columns must be passed as `Tuple`)


```julia
julia> most_delay(x, y) = x[topkperm(y, 2)]
julia> @chain flights begin
         groupby(:IATA)
         combine((:FlightDate, :DepDelay) => most_delay => :Most_Delay)
         setformat!(2 => monthday)
       end
28×2 Dataset
 Row │ IATA      Most_Delay
     │ identity  monthday   
     │ String?   Date?      
─────┼──────────────────────
   1 │ AA        (3, 7)
   2 │ AA        (8, 7)
   3 │ AS        (1, 22)
   4 │ AS        (10, 11)
   5 │ B6        (2, 9)
   6 │ B6        (5, 4)
   7 │ DL        (10, 8)
   8 │ DL        (6, 29)
   9 │ F9        (2, 3)
  10 │ F9        (1, 2)
  ⋮  │    ⋮          ⋮
  19 │ OO        (1, 10)
  20 │ OO        (1, 12)
  21 │ UA        (1, 20)
  22 │ UA        (1, 3)
  23 │ WN        (10, 26)
  24 │ WN        (9, 17)
  25 │ YV        (3, 6)
  26 │ YV        (1, 31)
  27 │ YX        (2, 1)
  28 │ YX        (2, 7)
              8 rows omitted
```

We could use Julia partial sorting too.

> **performance tip:** If you'll group often by the same column(s), you can use `groupby!` or `sort!` functions to sort your data set by that column(s) at once to optimise future computations. The difference between `groupby!` and `sort!` is that the former one sorts and marks data as grouped, but the latter one only sorts data.

For each month, calculate the number of flights and the change from the previous month

```julia
julia> @chain flights begin
          setformat!(1 => month)
          groupby(1)
          combine(1 => length => :length)
          modify!(:length => (x-> x .- lag(x)) => :change)
      end
12×3 Dataset
 Row │ FlightDate  length    change   
     │ month       identity  identity
     │ Date?       Int64?    Int64?   
─────┼────────────────────────────────
   1 │ 1              66748   missing
   2 │ 2              62504     -4244
   3 │ 3              69172      6668
   4 │ 4              31285    -37887
   5 │ 5              16940    -14345
   6 │ 6              22278      5338
   7 │ 7              33648     11370
   8 │ 8              34476       828
   9 │ 9              29825     -4651
  10 │ 10             32100      2275
  11 │ 11             33700      1600
  12 │ 12             34726      1026
```

## Visualising your data

The [StatsPlots](https://github.com/JuliaPlots/StatsPlots.jl), [VegaLite](https://github.com/queryverse/VegaLite.jl), [Makie](https://github.com/JuliaPlots/Makie.jl) packages (among others) make a rich set of visualisations possible with an intuitive syntax.

Here we use `VegaLite` to visualise the cancellation rate in busiest airports for each month:

```julia
julia> using VegaLite
julia> @chain flights begin
         groupby([:FlightDate, :Origin])
         combine(:Cancelled => mean => :rate)
         filter!(:Origin, by = in(["LAX", "SFO", "SAN"]))
         map(monthabbr, 1)
          _ |> @vlplot(:bar,
                        x = {"FlightDate:o", title = "Month", sort = false},
                        y = "rate",
                        column = "Origin:n",
                      )
       end
```

![cancellation](https://raw.githubusercontent.com/sl-solution/InMemoryDatasetsTutorial/main/flights.svg)

## Exporting data

You can use the `JLD2.jl` package to export the data set with meta information into a JLD2 file.

```julia
julia> using JLD2
julia> content(flights)
467402×17 Dataset
   Created: 2022-08-09T16:04:51.122
  Modified: 2022-08-09T16:05:03.822
      Info: Reporting Carrier On-Time Performance for all flights in 2020 from CA. Downloaded from www.transtats.bts.gov
-----------------------------------
Columns information 
┌─────┬──────────────────┬──────────┬─────────┐
│ Row │ col              │ format   │ eltype  │
├─────┼──────────────────┼──────────┼─────────┤
│   1 │ FlightDate       │ month    │ Date    │
│   2 │ IATA             │ identity │ String  │
│   3 │ Tail_Number      │ identity │ String  │
│   4 │ Flight_Number    │ identity │ Int64   │
│   5 │ Origin           │ identity │ String  │
│   6 │ Dest             │ identity │ String  │
│   7 │ TaxiOut          │ identity │ Float64 │
│   8 │ TaxiIn           │ identity │ Float64 │
│   9 │ DepDelay         │ identity │ Float64 │
│  10 │ ArrDelay         │ identity │ Float64 │
│  11 │ DepTime          │ identity │ Int64   │
│  12 │ ArrTime          │ identity │ Int64   │
│  13 │ AirTime          │ identity │ Float64 │
│  14 │ Cancelled        │ identity │ Float64 │
│  15 │ CancellationCode │ identity │ String  │
│  16 │ Distance         │ identity │ Float64 │
│  17 │ Speed            │ identity │ Float64 │
└─────┴──────────────────┴──────────┴─────────┘

julia> @save "flights.jld2" flights
julia> @load "flights.jld2" flights
1-element Vector{Symbol}:
 :flights

julia> content(flights)
467402×17 Dataset
   Created: 2022-08-09T16:04:51.122
  Modified: 2022-08-09T16:05:03.822
      Info: Reporting Carrier On-Time Performance for all flights in 2020 from CA. Downloaded from www.transtats.bts.gov
-----------------------------------
Columns information 
┌─────┬──────────────────┬──────────┬─────────┐
│ Row │ col              │ format   │ eltype  │
├─────┼──────────────────┼──────────┼─────────┤
│   1 │ FlightDate       │ month    │ Date    │
│   2 │ IATA             │ identity │ String  │
│   3 │ Tail_Number      │ identity │ String  │
│   4 │ Flight_Number    │ identity │ Int64   │
│   5 │ Origin           │ identity │ String  │
│   6 │ Dest             │ identity │ String  │
│   7 │ TaxiOut          │ identity │ Float64 │
│   8 │ TaxiIn           │ identity │ Float64 │
│   9 │ DepDelay         │ identity │ Float64 │
│  10 │ ArrDelay         │ identity │ Float64 │
│  11 │ DepTime          │ identity │ Int64   │
│  12 │ ArrTime          │ identity │ Int64   │
│  13 │ AirTime          │ identity │ Float64 │
│  14 │ Cancelled        │ identity │ Float64 │
│  15 │ CancellationCode │ identity │ String  │
│  16 │ Distance         │ identity │ Float64 │
│  17 │ Speed            │ identity │ Float64 │
└─────┴──────────────────┴──────────┴─────────┘
```

Also, you can use `DLMReader` package to write an `AbstractDataset` as text to a file using a given delimiter (which defaults to comma).

```julia
julia> using DLMReader
julia> filewriter("flights.csv", flights)
```

To write the formatted values, you need to use `filewriter` with `mapformats = true` option. For more information, see `?filewriter`.
