
using Test, InMemoryDatasets, Random, CategoricalArrays, PooledArrays
const ≅ = isequal

DATE(x) = Date(x)
DATE(::Missing) = missing

isequal_coltyped(ds1::AbstractDataset, ds2::AbstractDataset) =
    isequal(ds1, ds2) && typeof.(eachcol(ds1)) == typeof.(eachcol(ds2))

name = Dataset(ID = Union{Int, Missing}[1, 2, 3],
                Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
job = Dataset(ID = Union{Int, Missing}[1, 2, 2, 4],
                Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])

# Test output of various join types
outer = Dataset(ID = [1, 2, 2, 3, 4],
                  Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                  Job = ["Lawyer", "Doctor", "Florist", missing, "Farmer"])

# (Tests use current column ordering but don't promote it)
right = outer[Bool[!ismissing(x) for x in outer.Job], [:ID, :Name, :Job]]
left = outer[Bool[!ismissing(x) for x in outer.Name], :]
inner = left[Bool[!ismissing(x) for x in left.Job], :]
semi = unique(inner[:, [:ID, :Name]])
anti = left[Bool[ismissing(x) for x in left.Job], [:ID, :Name]]

classA = Dataset(id = ["id1", "id2", "id3", "id4", "id5"],
                        mark = [50, 69.5, 45.5, 88.0, 98.5])
grades = Dataset(mark = [0, 49.5, 59.5, 69.5, 79.5, 89.5, 95.5],
                        grade = ["F", "P", "C", "B", "A-", "A", "A+"])
closeone = Dataset(id = ["id1", "id2", "id3", "id4", "id5"],
                        mark = [50, 69.5, 45.5, 88.0, 98.5],
                        grade = ["P", "B", "F", "A-", "A+"])
trades = Dataset(
                [["20160525 13:30:00.023",
                  "20160525 13:30:00.038",
                  "20160525 13:30:00.048",
                  "20160525 13:30:00.048",
                  "20160525 13:30:00.048"],
                ["MSFT", "MSFT",
                 "GOOG", "GOOG", "AAPL"],
                [51.95, 51.95,
                 720.77, 720.92, 98.00],
                [75, 155,
                 100, 100, 100]],
               ["time", "ticker", "price", "quantity"]);
modify!(trades, 1 => byrow(x -> DateTime(x, dateformat"yyyymmdd HH:MM:SS.s")));
quotes = Dataset(
              [["20160525 13:30:00.023",
                "20160525 13:30:00.023",
                "20160525 13:30:00.030",
                "20160525 13:30:00.041",
                "20160525 13:30:00.048",
                "20160525 13:30:00.049",
                "20160525 13:30:00.072",
                "20160525 13:30:00.075"],
              ["GOOG", "MSFT", "MSFT", "MSFT",
               "GOOG", "AAPL", "GOOG", "MSFT"],
              [720.50, 51.95, 51.97, 51.99,
               720.50, 97.99, 720.50, 52.01],
              [720.93, 51.96, 51.98, 52.00,
               720.93, 98.01, 720.88, 52.03]],
             ["time", "ticker", "bid", "ask"]);
modify!(quotes, 1 => byrow(x -> DateTime(x, dateformat"yyyymmdd HH:MM:SS.s")));
closefinance1 = Dataset([Union{Missing, DateTime}[DateTime("2016-05-25T13:30:00.023"), DateTime("2016-05-25T13:30:00.038"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048")],
     Union{Missing, String}["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"],
     Union{Missing, Float64}[51.95, 51.95, 720.77, 720.92, 98.0],
     Union{Missing, Int64}[75, 155, 100, 100, 100],
     Union{Missing, String}["MSFT", "MSFT", "GOOG", "GOOG", "GOOG"],
     Union{Missing, Float64}[51.95, 51.97, 720.5, 720.5, 720.5],
     Union{Missing, Float64}[51.96, 51.98, 720.93, 720.93, 720.93]],["time", "ticker", "price", "quantity", "ticker_1", "bid", "ask"])
closfinance_tol2ms = Dataset([Union{Missing, DateTime}[DateTime("2016-05-25T13:30:00.023"), DateTime("2016-05-25T13:30:00.038"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048")], Union{Missing, String}["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"], Union{Missing, Float64}[51.95, 51.95, 720.77, 720.92, 98.0], Union{Missing, Int64}[75, 155, 100, 100, 100], Union{Missing, Float64}[51.95, missing, 720.5, 720.5, missing], Union{Missing, Float64}[51.96, missing, 720.93, 720.93, missing]], ["time", "ticker", "price", "quantity", "bid", "ask"])
closfinance_tol0ms = Dataset([Union{Missing, DateTime}[DateTime("2016-05-25T13:30:00.023"), DateTime("2016-05-25T13:30:00.038"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048")], Union{Missing, String}["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"], Union{Missing, Float64}[51.95, 51.95, 720.77, 720.92, 98.0], Union{Missing, Int64}[75, 155, 100, 100, 100], Union{Missing, Float64}[missing, missing, missing, missing, missing], Union{Missing, Float64}[missing, missing, missing, missing, missing]], ["time", "ticker", "price", "quantity", "bid", "ask"])

closefinance_tol10ms_noexact = Dataset([Union{Missing, DateTime}[DateTime("2016-05-25T13:30:00.023"), DateTime("2016-05-25T13:30:00.038"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048"), DateTime("2016-05-25T13:30:00.048")], Union{Missing, String}["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"], Union{Missing, Float64}[51.95, 51.95, 720.77, 720.92, 98.0], Union{Missing, Int64}[75, 155, 100, 100, 100], Union{Missing, Float64}[missing, 51.97, missing, missing, missing], Union{Missing, Float64}[missing, 51.98, missing, missing, missing]], ["time", "ticker", "price", "quantity", "bid", "ask"])

@testset "general usage" begin
    # Join on symbols or vectors of symbols
    innerjoin(name, job, on = :ID)
    innerjoin(name, job, on = [:ID])

    @test_throws ArgumentError innerjoin(name, job)
    @test_throws MethodError innerjoin(name, job, on = :ID, matchmissing=:errors)
    @test_throws MethodError outerjoin(name, job, on = :ID, matchmissing=:notequal)

    @test innerjoin(name, job, on = :ID) == inner == innerjoin(name, job, on = :ID, method = :hash)
    @test outerjoin(name, job, on = :ID) == outer == outerjoin(name, job, on = :ID, method = :hash)
    @test leftjoin(name, job, on = :ID) == left == leftjoin(name, job, on = :ID, method = :hash)
    @test semijoin(name, job, on = :ID) == semi == semijoin(name, job, on = :ID, method = :hash)
    @test antijoin(name, job, on = :ID) == anti == antijoin(name, job, on = :ID, method = :hash)
    @test closejoin(classA, grades, on = :mark) == closeone == closejoin(classA, grades, on = :mark, method = :hash)
    @test closejoin(trades, quotes, on = :time, makeunique = true) == closefinance1 == closejoin(trades, quotes, on = :time, makeunique = true, method = :hash)

    @test innerjoin(name, job, on = :ID) == inner == innerjoin(name, job, on = :ID, threads = false)
    @test innerjoin(name, job, on = :ID) == inner == innerjoin(name, job, on = :ID, method = :hash, threads = false)
    @test outerjoin(name, job, on = :ID) == outer == outerjoin(name, job, on = :ID, threads = false)
    @test outerjoin(name, job, on = :ID) == outer == outerjoin(name, job, on = :ID, method = :hash, threads = false)
    @test leftjoin(name, job, on = :ID) == left == leftjoin(name, job, on = :ID, threads = false)
    @test leftjoin(name, job, on = :ID) == left == leftjoin(name, job, on = :ID, method = :hash, threads = false)
    @test semijoin(name, job, on = :ID) == semi == semijoin(name, job, on = :ID, threads = false)
    @test semijoin(name, job, on = :ID) == semi == semijoin(name, job, on = :ID, method = :hash, threads = false)
    @test antijoin(name, job, on = :ID) == anti == antijoin(name, job, on = :ID, threads = false)
    @test antijoin(name, job, on = :ID) == anti == antijoin(name, job, on = :ID, method = :hash, threads = false)
    @test closejoin(classA, grades, on = :mark) == closeone == closejoin(classA, grades, on = :mark, threads = false)
    @test closejoin(classA, grades, on = :mark) == closeone == closejoin(classA, grades, on = :mark, method = :hash, threads = false)
    @test closejoin(trades, quotes, on = :time, makeunique = true) == closefinance1 == closejoin(trades, quotes, on = :time, makeunique = true, threads = false)
    @test closejoin(trades, quotes, on = :time, makeunique = true) == closefinance1 == closejoin(trades, quotes, on = :time, makeunique = true, method = :hash, threads = false)

    @test innerjoin(name, view(job, :, :), on = :ID) == inner
    @test outerjoin(name, view(job, :, :), on = :ID) == outer
    @test leftjoin(name, view(job, :, :), on = :ID) == left
    @test semijoin(name, view(job, :, :), on = :ID) == semi
    @test antijoin(name, view(job, :, :), on = :ID) == anti
    @test closejoin(classA, view(grades, :, :), on = :mark) == closeone
    @test closejoin(trades, view(quotes, :, :), on = :time, makeunique = true) == closefinance1
    @test innerjoin(name, view(job, :, :), on = :ID, method = :hash) == inner
    @test outerjoin(name, view(job, :, :), on = :ID, method = :hash) == outer
    @test leftjoin(name, view(job, :, :), on = :ID, method = :hash) == left
    @test semijoin(name, view(job, :, :), on = :ID, method = :hash) == semi
    @test antijoin(name, view(job, :, :), on = :ID, method = :hash) == anti
    @test closejoin(classA, view(grades, :, :), on = :mark, method = :hash) == closeone
    @test closejoin(trades, view(quotes, :, :), on = :time, makeunique = true, method = :hash) == closefinance1

    @test innerjoin(name, view(job, :, :), on = :ID, threads = false) == inner
    @test innerjoin(name, view(job, :, :), on = :ID, method = :hash, threads = false) == inner
    @test outerjoin(name, view(job, :, :), on = :ID, threads = false) == outer
    @test outerjoin(name, view(job, :, :), on = :ID, method = :hash, threads = false) == outer
    @test leftjoin(name, view(job, :, :), on = :ID, threads = false) == left
    @test leftjoin(name, view(job, :, :), on = :ID, method = :hash, threads = false) == left
    @test semijoin(name, view(job, :, :), on = :ID, threads = false) == semi
    @test semijoin(name, view(job, :, :), on = :ID, method = :hash, threads = false) == semi
    @test antijoin(name, view(job, :, :), on = :ID, threads = false) == anti
    @test antijoin(name, view(job, :, :), on = :ID, method = :hash, threads = false) == anti
    @test closejoin(classA, view(grades, :, :), on = :mark, threads = false) == closeone
    @test closejoin(classA, view(grades, :, :), on = :mark, method = :hash, threads = false) == closeone
    @test closejoin(trades, view(quotes, :, :), on = :time, makeunique = true, threads = false) == closefinance1
    @test closejoin(trades, view(quotes, :, :), on = :time, makeunique = true, method = :hash, threads = false) == closefinance1


    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(2)) == closfinance_tol2ms
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Day(2)) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(0)) == closfinance_tol0ms
    @test closejoin(trades, quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(2)) == closfinance_tol2ms
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Day(2)) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(0)) == closfinance_tol0ms
    @test closejoin!(copy(trades), quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false) == closefinance_tol10ms_noexact

    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(2), method = :hash) == closfinance_tol2ms
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Day(2), method = :hash) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(0), method = :hash) == closfinance_tol0ms
    @test closejoin(trades, quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(2), method = :hash) == closfinance_tol2ms
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Day(2), method = :hash) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(0), method = :hash) == closfinance_tol0ms
    @test closejoin!(copy(trades), quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash) == closefinance_tol10ms_noexact

    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(2),threads = false) == closfinance_tol2ms
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(2),method = :hash, threads = false) == closfinance_tol2ms
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Day(2),threads = false) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Day(2),method = :hash, threads = false) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(0),threads = false) == closfinance_tol0ms
    @test closejoin(trades, quotes, on =[:ticker, :time], tol = Millisecond(0),method = :hash, threads = false) == closfinance_tol0ms
    @test closejoin(trades, quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false,threads = false) == closefinance_tol10ms_noexact
    @test closejoin(trades, quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false,method = :hash, threads = false) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(2),threads = false) == closfinance_tol2ms
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(2),method = :hash, threads = false) == closfinance_tol2ms
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Day(2),threads = false) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Day(2),method = :hash, threads = false) == closejoin(trades, quotes, on =[:ticker, :time])
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(0),threads = false) == closfinance_tol0ms
    @test closejoin!(copy(trades), quotes, on =[:ticker, :time], tol = Millisecond(0),method = :hash, threads = false) == closfinance_tol0ms
    @test closejoin!(copy(trades), quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false,threads = false) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), quotes, on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false,method = :hash, threads = false) == closefinance_tol10ms_noexact


    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2)) == closfinance_tol2ms
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Day(2)) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0)) == closfinance_tol0ms
    @test closejoin(trades, view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2)) == closfinance_tol2ms
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Day(2)) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0)) == closfinance_tol0ms
    @test closejoin!(copy(trades), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false) == closefinance_tol10ms_noexact

    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash) == closfinance_tol2ms
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Day(2), method = :hash) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), method = :hash) == closfinance_tol0ms
    @test closejoin(trades, view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash) == closfinance_tol2ms
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Day(2), method = :hash) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), method = :hash) == closfinance_tol0ms
    @test closejoin!(copy(trades), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash) == closefinance_tol10ms_noexact

    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), threads = false) == closfinance_tol2ms
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash, threads = false) == closfinance_tol2ms
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Day(2), threads = false) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Day(2), method = :hash, threads = false) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), threads = false) == closfinance_tol0ms
    @test closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), method = :hash, threads = false) == closfinance_tol0ms
    @test closejoin(trades, view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, threads = false) == closefinance_tol10ms_noexact
    @test closejoin(trades, view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash, threads = false) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), threads = false) == closfinance_tol2ms
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash, threads = false) == closfinance_tol2ms
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Day(2), threads = false) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Day(2), method = :hash, threads = false) == closejoin(trades, view(quotes, :, :), on =[:ticker, :time])
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), threads = false) == closfinance_tol0ms
    @test closejoin!(copy(trades), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), method = :hash, threads = false) == closfinance_tol0ms
    @test closejoin!(copy(trades), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, threads = false) == closefinance_tol10ms_noexact
    @test closejoin!(copy(trades), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash, threads = false) == closefinance_tol10ms_noexact

    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :sort) == closfinance_tol2ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Day(2), method = :sort) == closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), method = :sort) == closfinance_tol0ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :sort) == closefinance_tol10ms_noexact

    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash) == closfinance_tol2ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Day(2), method = :hash) == closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), method = :hash) == closfinance_tol0ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash) == closefinance_tol10ms_noexact

    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), threads = false) == closfinance_tol2ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash, threads = false) == closfinance_tol2ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Day(2), threads = false) == closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Day(2), method = :hash, threads = false) == closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time])
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), threads = false) == closfinance_tol0ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(0), method = :hash, threads = false) == closfinance_tol0ms
    @test closejoin(view(trades, :, :), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, threads = false) == closefinance_tol10ms_noexact
    @test closejoin(view(trades, :, :), view(quotes, :, :), on = [:ticker, :time], tol = Millisecond(10), allow_exact_match = false, method = :hash, threads = false) == closefinance_tol10ms_noexact


    ANS = closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2))

    @test ANS.price.val !== trades.price.val
    cpy_trades = copy(trades)
    closejoin!(cpy_trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2))
    @test ANS.price.val !== trades.price.val

    ANS = closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash)
    ANS = closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), threads = false)
    ANS = closejoin(trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash, threads = false)


    @test ANS.price.val !== trades.price.val
    cpy_trades = copy(trades)
    closejoin!(cpy_trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash)
    cpy_trades = copy(trades)
    closejoin!(cpy_trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), threads = false)
    cpy_trades = copy(trades)
    closejoin!(cpy_trades, view(quotes, :, :), on =[:ticker, :time], tol = Millisecond(2), method = :hash, threads = false)

    @test ANS.price.val !== trades.price.val

    # Join with no non-key columns
    on = [:ID]
    nameid = name[:, on]
    jobid = job[:, on]

    @test innerjoin(nameid, jobid, on = :ID) == inner[:, on]
    @test outerjoin(nameid, jobid, on = :ID) == outer[:, on]
    @test leftjoin(nameid, jobid, on = :ID) == left[:, on]
    @test semijoin(nameid, jobid, on = :ID) == semi[:, on]
    @test antijoin(nameid, jobid, on = :ID) == anti[:, on]
    @test innerjoin(nameid, view(jobid, :, :), on = :ID) == inner[:, on]
    @test outerjoin(nameid, view(jobid, :, :), on = :ID) == outer[:, on]
    @test leftjoin(nameid, view(jobid, :, :), on = :ID) == left[:, on]
    @test innerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID) == inner[:, on]
    @test outerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID) == outer[:, on]
    @test leftjoin(view(nameid, :, :), view(jobid, :, :), on = :ID) == left[:, on]
    @test semijoin(nameid, view(jobid, :, :), on = :ID) == semi[:, on]
    @test antijoin(nameid, view(jobid, :, :), on = :ID) == anti[:, on]

    @test innerjoin(nameid, jobid, on = :ID, method = :hash) == inner[:, on]
    @test outerjoin(nameid, jobid, on = :ID, method = :hash) == outer[:, on]
    @test leftjoin(nameid, jobid, on = :ID, method = :hash) == left[:, on]
    @test semijoin(nameid, jobid, on = :ID, method = :hash) == semi[:, on]
    @test antijoin(nameid, jobid, on = :ID, method = :hash) == anti[:, on]
    @test innerjoin(nameid, view(jobid, :, :), on = :ID, method = :hash) == inner[:, on]
    @test outerjoin(nameid, view(jobid, :, :), on = :ID, method = :hash) == outer[:, on]
    @test leftjoin(nameid, view(jobid, :, :), on = :ID, method = :hash) == left[:, on]
    @test innerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, method = :hash) == inner[:, on]
    @test outerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, method = :hash) == outer[:, on]
    @test leftjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, method = :hash) == left[:, on]
    @test semijoin(nameid, view(jobid, :, :), on = :ID, method = :hash) == semi[:, on]
    @test antijoin(nameid, view(jobid, :, :), on = :ID, method = :hash) == anti[:, on]

    @test innerjoin(nameid, jobid, on = :ID, threads = false) == inner[:, on]
    @test innerjoin(nameid, jobid, on = :ID, method = :hash, threads = false) == inner[:, on]
    @test outerjoin(nameid, jobid, on = :ID, threads = false) == outer[:, on]
    @test outerjoin(nameid, jobid, on = :ID, method = :hash, threads = false) == outer[:, on]
    @test leftjoin(nameid, jobid, on = :ID, threads = false) == left[:, on]
    @test leftjoin(nameid, jobid, on = :ID, method = :hash, threads = false) == left[:, on]
    @test semijoin(nameid, jobid, on = :ID, threads = false) == semi[:, on]
    @test semijoin(nameid, jobid, on = :ID, method = :hash, threads = false) == semi[:, on]
    @test antijoin(nameid, jobid, on = :ID, threads = false) == anti[:, on]
    @test antijoin(nameid, jobid, on = :ID, method = :hash, threads = false) == anti[:, on]
    @test innerjoin(nameid, view(jobid, :, :), on = :ID, threads = false) == inner[:, on]
    @test innerjoin(nameid, view(jobid, :, :), on = :ID, method = :hash, threads = false) == inner[:, on]
    @test outerjoin(nameid, view(jobid, :, :), on = :ID, threads = false) == outer[:, on]
    @test outerjoin(nameid, view(jobid, :, :), on = :ID, method = :hash, threads = false) == outer[:, on]
    @test leftjoin(nameid, view(jobid, :, :), on = :ID, threads = false) == left[:, on]
    @test leftjoin(nameid, view(jobid, :, :), on = :ID, method = :hash, threads = false) == left[:, on]
    @test innerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, threads = false) == inner[:, on]
    @test innerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, method = :hash, threads = false) == inner[:, on]
    @test outerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, threads = false) == outer[:, on]
    @test outerjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, method = :hash, threads = false) == outer[:, on]
    @test leftjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, threads = false) == left[:, on]
    @test leftjoin(view(nameid, :, :), view(jobid, :, :), on = :ID, method = :hash, threads = false) == left[:, on]
    @test semijoin(nameid, view(jobid, :, :), on = :ID, threads = false) == semi[:, on]
    @test semijoin(nameid, view(jobid, :, :), on = :ID, method = :hash, threads = false) == semi[:, on]
    @test antijoin(nameid, view(jobid, :, :), on = :ID, threads = false) == anti[:, on]
    @test antijoin(nameid, view(jobid, :, :), on = :ID, method = :hash, threads = false) == anti[:, on]

    # Join on multiple keys
    ds1 = Dataset(A = 1, B = 2, C = 3)
    ds2 = Dataset(A = 1, B = 2, D = 4)

    @test innerjoin(ds1, ds2, on = [:A, :B]) == Dataset(A = 1, B = 2, C = 3, D = 4)
    @test innerjoin(ds1, view(ds2, :, :), on = [:A, :B]) == Dataset(A = 1, B = 2, C = 3, D = 4)

    @test innerjoin(ds1, ds2, on = [:A, :B], method = :hash) == Dataset(A = 1, B = 2, C = 3, D = 4)
    @test innerjoin(ds1, view(ds2, :, :), on = [:A, :B], method = :hash) == Dataset(A = 1, B = 2, C = 3, D = 4)
    @test innerjoin(ds1, view(ds2, :, :), on = [:A, :B], threads = false) == Dataset(A = 1, B = 2, C = 3, D = 4)
    @test innerjoin(ds1, view(ds2, :, :), on = [:A, :B], method = :hash, threads = false) == Dataset(A = 1, B = 2, C = 3, D = 4)


    dsl = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
         Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
         Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
         Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], ["x1", "x2", "x3", "row"])
    dsr = Dataset(x1=[1, 3], y =[100.0, 200.0])
    setformat!(dsl, 1=>iseven)
    setformat!(dsr, 1=>isodd)

    left1 = leftjoin(dsl, dsr, on = :x1)
    left1_v = leftjoin(dsl, view(dsr, :, [2,1]), on = :x1)

    left1_t = Dataset([Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[3, 3, 6, 7, 7, 10, 10, 5, 10, 10, 9, 1, 1, 1, 1],
           Union{Missing, Int64}[1, 1, 2, 3, 3, 4, 5, 6, 7, 7, 8, 9, 9, 10, 10],
           Union{Missing, Float64}[100.0, 200.0, missing, 100.0, 200.0, missing, missing, missing, 100.0, 200.0, missing, 100.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    left2 = leftjoin(dsl, dsr, on = :x1, mapformats = [true, false])
    left2_v = leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false])
    @test left2 == leftjoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash)
    @test left2_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash)

    @test left2 == leftjoin(dsl, dsr, on = :x1, mapformats = [true, false], threads = false)
    @test left2 == leftjoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash, threads = false)
    @test left2_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], threads = false)
    @test left2_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash, threads = false)

    left2_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, missing, 100.0, missing, missing, missing, 100.0, missing, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    left3 = leftjoin(dsl, dsr, on = :x1, mapformats = [false, true])
    left3_v = leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true])

    @test left3 == leftjoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash)
    @test left3_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash)

    @test left3 == leftjoin(dsl, dsr, on = :x1, mapformats = [false, true], threads = false)
    @test left3 == leftjoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash, threads = false)
    @test left3_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], threads = false)
    @test left3_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash, threads = false)

    left3_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, missing, missing, 100.0, 200.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    left4 = leftjoin(dsl, dsr, on = :x1, mapformats = [false, false])
    left4_v = leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false])

    @test left4 == leftjoin(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash)
    @test left4_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash)

    @test left4 == leftjoin(dsl, dsr, on = :x1, mapformats = [false, false], threads = false)
    @test left4 == leftjoin(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash, threads = false)
    @test left4_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], threads = false)
    @test left4_v == leftjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash, threads = false)

    left4_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, 200.0, missing, 100.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    inner1 = innerjoin(dsl, dsr, on = :x1)
    inner1_v = innerjoin(dsl, view(dsr, :, [2,1]), on = :x1)

    @test inner1 == innerjoin(dsl, dsr, on = :x1, method = :hash)
    @test inner1_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, method = :hash)

    @test inner1 == innerjoin(dsl, dsr, on = :x1, threads = false)
    @test inner1 == innerjoin(dsl, dsr, on = :x1, method = :hash, threads = false)
    @test inner1_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, threads = false)
    @test inner1_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, method = :hash, threads = false)

    inner1_t = Dataset([Union{Missing, Int64}[10, 10, 4, 4, 6, 6, 2, 2, 10, 10],
           Union{Missing, Int64}[10, 10, 4, 4, 6, 6, 2, 2, 10, 10],
           Union{Missing, Int64}[3, 3, 7, 7, 10, 10, 1, 1, 1, 1],
           Union{Missing, Int64}[1, 1, 3, 3, 7, 7, 9, 9, 10, 10],
           Union{Missing, Float64}[100.0, 200.0, 100.0, 200.0, 100.0, 200.0, 100.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    inner2 = innerjoin(dsl, dsr, on = :x1, mapformats = [true, false])
    inner2_v = innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false])

    @test inner2 == innerjoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash)
    @test inner2_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash)

    @test inner2 == innerjoin(dsl, dsr, on = :x1, mapformats = [true, false], threads = false)
    @test inner2 == innerjoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash, threads = false)
    @test inner2_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], threads = false)
    @test inner2_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash, threads = false)

    inner2_t = Dataset([ Union{Missing, Int64}[10, 4, 6, 2, 10],
           Union{Missing, Int64}[10, 4, 6, 2, 10],
           Union{Missing, Int64}[3, 7, 10, 1, 1],
           Union{Missing, Int64}[1, 3, 7, 9, 10],
           Union{Missing, Float64}[100.0, 100.0, 100.0, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    inner3 = innerjoin(dsl, dsr, on = :x1, mapformats = [false, true])
    inner3_v = innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true])

    @test inner3 == innerjoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash)
    @test inner3_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash)

    @test inner3 == innerjoin(dsl, dsr, on = :x1, mapformats = [false, true], threads = false)
    @test inner3 == innerjoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash, threads = false)
    @test inner3_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], threads = false)
    @test inner3_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash, threads = false)

    inner3_t = Dataset([Union{Missing, Int64}[1, 1],
           Union{Missing, Int64}[1, 1],
           Union{Missing, Int64}[10, 10],
           Union{Missing, Int64}[4, 4],
           Union{Missing, Float64}[100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    inner4 = innerjoin(dsl, dsr, on = :x1, mapformats = [false, false])
    inner4_v = innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false])

    @test inner4 == innerjoin(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash)
    @test inner4_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash)

    @test inner4 == innerjoin(dsl, dsr, on = :x1, mapformats = [false, false], threads = false)
    @test inner4 == innerjoin(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash, threads = false)
    @test inner4_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], threads = false)
    @test inner4_v == innerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash, threads = false)

    inner4_t = Dataset([Union{Missing, Int64}[3, 1],
           Union{Missing, Int64}[3, 1],
           Union{Missing, Int64}[6, 10],
           Union{Missing, Int64}[2, 4],
           Union{Missing, Float64}[200.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    outer1 = outerjoin(dsl, dsr, on = :x1)
    outer1_v = outerjoin(dsl, view(dsr, :, [2,1]), on = :x1)

    @test outer1 == outerjoin(dsl, dsr, on = :x1, method = :hash)
    @test outer1_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, method = :hash)

    @test outer1 == outerjoin(dsl, dsr, on = :x1, threads = false)
    @test outer1 == outerjoin(dsl, dsr, on = :x1, method = :hash, threads = false)
    @test outer1_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, threads = false)
    @test outer1_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, method = :hash, threads = false)

    outer1_t = Dataset([Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[3, 3, 6, 7, 7, 10, 10, 5, 10, 10, 9, 1, 1, 1, 1],
           Union{Missing, Int64}[1, 1, 2, 3, 3, 4, 5, 6, 7, 7, 8, 9, 9, 10, 10],
           Union{Missing, Float64}[100.0, 200.0, missing, 100.0, 200.0, missing, missing, missing, 100.0, 200.0, missing, 100.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    outer2 = outerjoin(dsl, dsr, on = :x1, mapformats = [false, true])
    outer2_v = outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true])

    @test outer2 == outerjoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash)
    @test outer2_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash)

    @test outer2 == outerjoin(dsl, dsr, on = :x1, mapformats = [false, true], threads = false)
    @test outer2 == outerjoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash, threads = false)
    @test outer2_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], threads = false)
    @test outer2_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash, threads = false)

    outer2_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, missing, missing, 100.0, 200.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    outer3 = outerjoin(dsl, dsr, on = :x1, mapformats = [true, false])
    outer3_v = outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false])

    @test outer3 == outerjoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash)
    @test outer3_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash)

    @test outer3 == outerjoin(dsl, dsr, on = :x1, mapformats = [true, false], threads = false)
    @test outer3 == outerjoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash, threads = false)
    @test outer3_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], threads = false)
    @test outer3_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash, threads = false)

    outer3_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10, 3],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10, missing],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1, missing],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, missing],
           Union{Missing, Float64}[100.0, missing, 100.0, missing, missing, missing, 100.0, missing, 100.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    outer4 = outerjoin(dsl, dsr, on = :x1, mapformats = [false, false])
    outer4_v = outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false])

    @test outer4 == outerjoin(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash)
    @test outer4_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash)

    @test outer4 == outerjoin(dsl, dsr, on = :x1, mapformats = [false, false], threads =false)
    @test outer4_v == outerjoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], threads =false)

    outer4_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, 200.0, missing, 100.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    contains1 = contains(dsl, dsr, on = :x1)
    contains1_v = contains(dsl, view(dsr, :, [2,1]), on = :x1)

    @test contains1 == contains(dsl, dsr, on = :x1, method = :hash)
    @test contains1_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, method = :hash)

    @test contains1 == contains(dsl, dsr, on = :x1, threads = false)
    @test contains1 == contains(dsl, dsr, on = :x1, method = :hash, threads = false)
    @test contains1_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, threads = false)
    @test contains1_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, method = :hash, threads = false)

    contains1_t = Bool[1, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    contains2 = contains(dsl, dsr, on = :x1, mapformats = [true, false])
    contains2_v = contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false])

    @test contains2 == contains(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash)
    @test contains2_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash)

    @test contains2 == contains(dsl, dsr, on = :x1, mapformats = [true, false], threads = false)
    @test contains2 == contains(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash, threads = false)
    @test contains2_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], threads = false)
    @test contains2_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash, threads = false)

    contains2_t = Bool[1, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    contains3 = contains(dsl, dsr, on = :x1, mapformats =[false, true])
    contains3_v = contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats =[false, true])

    @test contains3 == contains(dsl, dsr, on = :x1, mapformats =[false, true], method = :hash)
    @test contains3_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats =[false, true], method = :hash)

    @test contains3 == contains(dsl, dsr, on = :x1, mapformats =[false, true], threads = false)
    @test contains3 == contains(dsl, dsr, on = :x1, mapformats =[false, true], method = :hash, threads = false)
    @test contains3_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats =[false, true], threads = false)
    @test contains3_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats =[false, true], method = :hash, threads = false)

    contains3_t = Bool[0, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    contains4 = contains(dsl, dsr, on = :x1, mapformats = [false, false])
    contains4_v = contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false])

    @test contains4 == contains(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash)
    @test contains4_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash)
    @test contains4 == contains(dsl, dsr, on = :x1, mapformats = [false, false], threads = false)
    @test contains4 == contains(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash, threads = false)
    @test contains4_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], threads = false)
    @test contains4_v == contains(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash, threads = false)

    contains4_t = Bool[0, 1, 0, 1, 0, 0, 0, 0, 0, 0]

    close1 = closejoin(dsl, dsr, on = :x1)
    close1_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1)

    @test close1 == closejoin(dsl, dsr, on = :x1, method = :hash)
    @test close1_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, method = :hash)

    @test close1 == closejoin(dsl, dsr, on = :x1, threads =false)
    @test close1_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, threads =false)

    close1_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, missing, 200.0, missing, missing, missing, 200.0, missing, 200.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close2 = closejoin(dsl, dsr, on = :x1, direction = :forward)
    close2_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, direction = :forward)

    @test close2 == closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash)
    @test close2_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, direction = :forward, method = :hash)

    @test close2 == closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false)
    @test close2 == closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false)
    @test close2_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, direction = :forward, threads = false)
    @test close2_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, direction = :forward, method = :hash, threads = false)

    close2_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    close3 = closejoin(dsl, dsr, on = :x1, border = :nearest)
    close3_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, border = :nearest)

    @test close3 == closejoin(dsl, dsr, on = :x1, border = :nearest, method = :hash)
    @test close3_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, border = :nearest, method = :hash)

    @test close3 == closejoin(dsl, dsr, on = :x1, border = :nearest, threads = false)
    @test close3 == closejoin(dsl, dsr, on = :x1, border = :nearest, method = :hash, threads = false)
    @test close3_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, border = :nearest, threads = false)
    @test close3_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, border = :nearest, method = :hash, threads = false)

    close3_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 100.0, 200.0, 100.0, 100.0, 100.0, 200.0, 100.0, 200.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close4 = closejoin(dsl, dsr, on = :x1, mapformats = [true, false])
    close4_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false])

    @test close4 == closejoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash)
    @test close4_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash)

    @test close4 == closejoin(dsl, dsr, on = :x1, mapformats = [true, false], threads = false)
    @test close4 == closejoin(dsl, dsr, on = :x1, mapformats = [true, false], method = :hash, threads = false)
    @test close4_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], threads = false)
    @test close4_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], method = :hash, threads = false)

    close4_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, missing, 100.0, missing, missing, missing, 100.0, missing, 100.0, 100.0]],  ["x1", "x2", "x3", "row", "y"])
    close5 = closejoin(dsl, dsr, on = :x1, mapformats = [true, false], direction = :forward)
    close5_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], direction = :forward)

    @test close5 == closejoin(dsl, dsr, on = :x1, mapformats = [true, false], direction = :forward, method = :hash)
    @test close5_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], direction = :forward, method = :hash)

    @test close5 == closejoin(dsl, dsr, on = :x1, mapformats = [true, false], direction = :forward, threads = false)
    @test close5 == closejoin(dsl, dsr, on = :x1, mapformats = [true, false], direction = :forward, method = :hash, threads = false)
    @test close5_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], direction = :forward, threads = false)
    @test close5_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [true, false], direction = :forward, method = :hash, threads = false)

    close5_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    close6 = closejoin(dsl, dsr, on = :x1, mapformats = [false, true])
    close6_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true])

    @test close6 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash)
    @test close6_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash)

    @test close6 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], threads = false)
    @test close6 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], method = :hash, threads = false)
    @test close6_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], threads = false)
    @test close6_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], method = :hash, threads = false)

    close6_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close7 = closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward)
    close7_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward)

    @test close7 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, method = :hash)
    @test close7_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward, method = :hash)

    @test close7 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, threads = false)
    @test close7 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, method = :hash, threads = false)
    @test close7_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward, threads = false)
    @test close7_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward, method = :hash, threads = false)


    close7_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, missing, missing, 100.0, missing, missing, missing, missing, missing, missing]],["x1", "x2", "x3", "row", "y"])
    close8 = closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, border = :nearest)
    close8_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward, border = :nearest)

    @test close8 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, border = :nearest, method = :hash)
    @test close8_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward, border = :nearest, method = :hash)

    @test close8 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, border = :nearest, threads = false)
    @test close8 == closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, border = :nearest, method = :hash, threads = false)
    @test close8_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward, border = :nearest, threads = false)
    @test close8_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, true], direction = :forward, border = :nearest, method = :hash, threads = false)

    close8_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 200.0, 200.0, 100.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0]],["x1", "x2", "x3", "row", "y"])
    close9 = closejoin(dsl, dsr, on = :x1, mapformats = [false, false])
    close9_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false])

    @test close9 == closejoin(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash)
    @test close9_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash)

    @test close9 == closejoin(dsl, dsr, on = :x1, mapformats = [false, false], threads = false)
    @test close9 == closejoin(dsl, dsr, on = :x1, mapformats = [false, false], method = :hash, threads = false)
    @test close9_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], threads = false)
    @test close9_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = [false, false], method = :hash, threads = false)

    close9_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 200.0, 200.0, 100.0, 200.0, 200.0, 200.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close10 = closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward)
    close10_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward)

    @test close10 == closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, method = :hash)
    @test close10_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward, method = :hash)

    @test close10 == closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, threads = false)
    @test close10 == closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, method = :hash, threads = false)
    @test close10_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward, threads = false)
    @test close10_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward, method = :hash, threads = false)

    close10_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, 200.0, missing, 100.0, missing, missing, missing, missing, 200.0, missing]], ["x1", "x2", "x3", "row", "y"])
    close11 = closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, border = :nearest)
    close11_v = closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward, border = :nearest)

    @test close11 == closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, border = :nearest, method = :hash)
    @test close11_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward, border = :nearest, method = :hash)

    @test close11 == closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, border = :nearest, threads = false)
    @test close11 == closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, border = :nearest, method = :hash, threads = false)
    @test close11_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward, border = :nearest, threads = false)
    @test close11_v == closejoin(dsl, view(dsr, :, [2,1]), on = :x1, mapformats = false, direction = :forward, border = :nearest, method = :hash, threads = false)

    close11_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 200.0, 200.0, 100.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    @test left1 == left1_t
    @test left2 == left2_t
    @test left3 == left3_t
    @test left4 == left4_t
    @test inner1 == inner1_t
    @test inner2 == inner2_t
    @test inner3 == inner3_t
    @test inner4 == inner4_t
    @test outer1 == outer1_t
    @test outer2 == outer2_t
    @test outer3 == outer3_t
    @test outer4 == outer4_t
    @test contains1 == contains1_t
    @test contains2 == contains2_t
    @test contains3 == contains3_t
    @test contains4 == contains4_t
    @test close1 == close1_t
    @test close2 == close2_t
    @test close3 == close3_t
    @test close4 == close4_t
    @test close5 == close5_t
    @test close6 == close6_t
    @test close7 == close7_t
    @test close8 == close8_t
    @test close9 == close9_t
    @test close10 == close10_t
    @test close11 == close11_t

    @test left1_v == left1_t
    @test left2_v == left2_t
    @test left3_v == left3_t
    @test left4_v == left4_t
    @test inner1_v == inner1_t
    @test inner2_v == inner2_t
    @test inner3_v == inner3_t
    @test inner4_v == inner4_t
    @test outer1_v == outer1_t
    @test outer2_v == outer2_t
    @test outer3_v == outer3_t
    @test outer4_v == outer4_t
    @test contains1_v == contains1_t
    @test contains2_v == contains2_t
    @test contains3_v == contains3_t
    @test contains4_v == contains4_t
    @test close1_v == close1_t
    @test close2_v == close2_t
    @test close3_v == close3_t
    @test close4_v == close4_t
    @test close5_v == close5_t
    @test close6_v == close6_t
    @test close7_v == close7_t
    @test close8_v == close8_t
    @test close9_v == close9_t
    @test close10_v == close10_t
    @test close11_v == close11_t

    dsl = Dataset([[Characters{1, UInt8}(randstring(1)) for _ in 1:10^5] for _ in 1:3], :auto)
    dsr = Dataset([[Characters{1, UInt8}(randstring(1)) for _ in 1:10^5] for _ in 1:3], :auto)
    left1 = leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = true, stable =true, check = false)
    left2 = leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false)

    @test left1 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = true, stable =true, check = false, method = :hash)
    @test left2 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, method = :hash)

    @test left1 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = true, stable =true, check = false, threads = false)
    @test left1 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = true, stable =true, check = false, method = :hash, threads = false)
    @test left2 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, threads = false)
    @test left2 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, method = :hash, threads = false)

    @test left1 == left2
    @test unique(select!(left1, [:x1, :x2, :x3]), [:x1, :x2]) == unique(dsl, [:x1, :x2])

    dsl = Dataset([[Characters{1, UInt8}(randstring(1)) for _ in 1:10^5] for _ in 1:3], :auto)
    dsr = Dataset([[Characters{1, UInt8}(randstring(1)) for _ in 1:10^5] for _ in 1:3], :auto)
    for i in 1:3
        dsl[!, i] = PooledArray(dsl[!, i])
        dsr[!, i] = PooledArray(dsr[!, i])
    end
    for i in 1:10
        left1 = leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = true, stable =true, check = false)
        left2 = leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false)
        @test left2 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, method = :hash)
        @test left2 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, threads = false)
        @test left2 == leftjoin(dsl, dsr, on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, method = :hash, threads = false)


        @test left1 == left2
        @test unique(select!(left1, [:x1, :x2, :x3]), [:x1, :x2]) == unique(dsl, [:x1, :x2])
        left1 = leftjoin(dsl, view(dsr, :, :), on = [:x1, :x2], makeunique = true, accelerate = true, stable =true, check = false)
        left2 = leftjoin(dsl, view(dsr, :, :), on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false)
        @test left2 == leftjoin(dsl, view(dsr, :, :), on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, method = :hash)
        @test left2 == leftjoin(dsl, view(dsr, :, :), on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, threads = false)
        @test left2 == leftjoin(dsl, view(dsr, :, :), on = [:x1, :x2], makeunique = true, accelerate = false, stable = true, check = false, method = :hash, threads = false)

        @test left1 == left2
        @test unique(select!(left1, [:x1, :x2, :x3]), [:x1, :x2]) == unique(dsl, [:x1, :x2])
    end

    x1 = rand(1:1000, 5000)
    x2 = rand(1:100, 5000)
    y = rand(5000)
    y2 = rand(5000)
    dsl = Dataset(x1 = Characters{6, UInt8}.(c"id" .* string.(x1)), x2 = Characters{5, UInt8}.(c"id" .* string.(x2)), y = y)
    dsr = Dataset(x1 = x1, x2 = x2, y2 = y2)
    fmtfun(x) = @views parse(Int, x[3:end])
    setformat!(dsl, 1:2=>fmtfun)
    semi1 = semijoin(dsl, dsr, on = [:x1, :x2])
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], method = :hash)
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], threads = false)
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], method = :hash, threads = false)

    semi2 = semijoin(dsl, dsr, on = [:x1, :x2], accelerate = true)
    @test semi1 == dsl
    @test semi2 == dsl
    semi1 = semijoin(dsl, view(dsr, :, :), on = [:x1, :x2])
    @test semi1 == semijoin(dsl, view(dsr, :, :), on = [:x1, :x2], method = :hash)
    @test semi1 == semijoin(dsl, view(dsr, :, :), on = [:x1, :x2], threads = false)
    @test semi1 == semijoin(dsl, view(dsr, :, :), on = [:x1, :x2], method = :hash, threads = false)


    semi2 = semijoin(dsl, view(dsr, :, :), on = [:x1, :x2], accelerate = true)
    @test semi1 == dsl
    @test semi2 == dsl
    inn1 = innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true)
    out1 = outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true)
    left1 = leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true, method = :hash)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, threads = false)
    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true, method = :hash, threads = false)

    @test inn1 == out1 == left1
    fmtfun2(x) = c"id" * Characters{4, UInt8}(x)
    setformat!(dsr, 1:2=>fmtfun2)
    semi1 = semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [false, true])
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [false, true], method = :hash)
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [false, true], threads = false)
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [false, true], method = :hash, threads = false)


    semi2 = semijoin(dsl, dsr, on = [:x1, :x2], accelerate = true, mapformats = [false, true])
    @test semi1 == dsl
    @test semi2 == dsl
    inn1 = innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true)
    out1 = outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true)
    left1 = leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true, method = :hash)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, threads = false)
    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true, method = :hash, threads = false)

    @test inn1 == out1 == left1
    x1 = rand(1:1000, 5000)
    x2 = rand(1:100, 5000)
    y = rand(5000)
    y2 = rand(5000)
    dsl = Dataset(x1 = Characters{6, UInt8}.(c"id" .* string.(x1)), x2 = Characters{5, UInt8}.(c"id" .* string.(x2)), y = y)
    dsr = Dataset(x1 = x1, x2 = x2, y2 = y2)
    for i in 1:2
        dsl[!, i] = PooledArray(dsl[!, i])
        dsr[!, i] = PooledArray(dsr[!, i])
    end
    setformat!(dsl, 1:2=>fmtfun)
    semi1 = semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [true, false])
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [true, false], method =:hash)
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [true, false], threads = false)
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [true, false], method = :hash, threads = false)


    semi2 = semijoin(dsl, dsr, on = [:x1, :x2], accelerate = true, mapformats = [true, false])
    @test semi1 == dsl
    @test semi2 == dsl
    inn1 = innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true)
    out1 = outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true)
    left1 = leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true, method = :hash)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, threads = false)
    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true, method = :hash, threads = false)

    @test inn1 == out1 == left1
    setformat!(dsr, 1:2=>fmtfun2)
    semi1 = semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [false, true])
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [false, true], method = :hash)

    semi2 = semijoin(dsl, dsr, on = [:x1, :x2], accelerate = true, mapformats = [false, true])
    @test semi1 == dsl
    @test semi2 == dsl
    inn1 = innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true)
    out1 = outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true)
    left1 = leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true, method = :hash)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, threads = false)
    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, threads = false)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], stable = true, method = :hash, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true, threads = false)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [false, true], accelerate = true, stable =true, method = :hash, threads = false)

    @test inn1 == out1 == left1
    x1 = -rand(1:1000, 5000)
    x2 = -rand(1:100, 5000)
    y = rand(5000)
    y2 = rand(5000)
    dsl = Dataset(x1 = Characters{6, UInt8}.(c"id" .* string.(-x1)), x2 = Characters{5, UInt8}.(c"id" .* string.(-x2)), y = y)
    dsr = Dataset(x1 = x1, x2 = x2, y2 = y2)
    for i in 1:2
        dsl[!, i] = PooledArray(dsl[!, i])
        dsr[!, i] = PooledArray(dsr[!, i])
    end
    fmtfun3(x) = @views -parse(Int, x[3:end])
    setformat!(dsl, 1:2=>fmtfun3)
    semi1 = semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [true, false])
    @test semi1 == semijoin(dsl, dsr, on = [:x1, :x2], mapformats = [true, false], method = :hash)

    semi2 = semijoin(dsl, dsr, on = [:x1, :x2], accelerate = true, mapformats = [true, false])
    @test semi1 == dsl
    @test semi2 == dsl
    inn1 = innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true)
    out1 = outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true)
    left1 = leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true)

    @test inn1 == innerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash)
    @test out1 == outerjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], stable = true, method = :hash)
    @test left1 == leftjoin(dsl, dsr, on =[:x1, :x2], mapformats = [true, false], accelerate = true, stable =true, method = :hash)

    @test inn1 == out1 == left1

    dsl = Dataset(x = [1,1,1,2,2,2], y = PooledArray([6,4,1,2,5,3]))
    dsr = Dataset(x = [1,1,2], y = PooledArray([0,3,1]), z=[100,200,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash,  makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash,  makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash,  makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    dsl = Dataset(x = [1,1,1,2,2,2], y = ([6,4,1,2,5,3]))
    dsr = Dataset(x = [1,1,2], y = PooledArray([0,3,1]), z=[100,200,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])


    dsl = Dataset(x = [1,1,1,2,2,2], y = PooledArray([6,4,1,2,5,3]))
    dsr = Dataset(x = [1,1,2], y = ([0,3,1]), z=[100,200,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    dsl = Dataset(x = PooledArray([1,1,1,2,2,2]), y = PooledArray([6,4,1,2,5,3]))
    dsr = Dataset(x = PooledArray([2,1,1]), y = PooledArray([1,3,0]), z=[300,200,100])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    dsl = Dataset(x = PooledArray([1,1,1,2,2,2]), y = ([6,4,1,2,5,3]))
    dsr = Dataset(x = PooledArray([2,1,1]), y = PooledArray([1,3,0]), z=[300,200,100])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    dsl = Dataset(x = PooledArray([1,1,1,2,2,2]), y = PooledArray([6,4,1,2,5,3]))
    dsr = Dataset(x = PooledArray([2,1,1]), y = PooledArray([1,3,0]), z=[300,200,100])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,100, 300,300,300])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[missing, missing, 200, missing, missing, missing])
    @test closejoin(dsl, dsr, on = [:x, :y], method = :hash, makeunique = true, direction = :forward, border = :nearest) == Dataset(x=[1,1,1,2,2,2], y=[6,4,1,2,5,3],z=[200,200,200, 300,300,300])

    #views
    for i in 1:100
        l_ridx= rand(1:100, 200)
        l_cidx = shuffle(1:3)
        dsl = Dataset(rand(1:10, 100, 3), :auto)
        dsr = Dataset(rand(1:10, 3, 2), :auto)
        @test leftjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1, :x2], makeunique=true, check = false) == leftjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1, :x2], makeunique=true, check = false)
        @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1, :x2], makeunique=true, check = false) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1, :x2], makeunique=true, check = false)
        @test outerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1, :x2], makeunique=true, check = false) == outerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1, :x2], makeunique=true, check = false)
        @test leftjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1], makeunique=true, check = false) == leftjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1], makeunique=true, check = false)
        @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1], makeunique=true, check = false) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1], makeunique=true, check = false)
        @test outerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1], makeunique=true, check = false) == outerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1], makeunique=true, check = false)
    end

    for i in 1:100
        l_ridx= rand(1:100, 200)
        l_cidx = shuffle(1:3)
        dsl = Dataset(rand(1:10, 100, 3), :auto)
        dsr = Dataset(rand(1:10, 3, 2), :auto)
        @test leftjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1, :x2], makeunique=true, check = false, method = :hash) == leftjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1, :x2], makeunique=true, check = false)
        @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1, :x2], makeunique=true, check = false, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1, :x2], makeunique=true, check = false)
        @test sort(outerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1, :x2], makeunique=true, check = false, method = :hash), :) == sort(outerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1, :x2], makeunique=true, check = false), :)
        @test leftjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1], makeunique=true, check = false, method = :hash) == leftjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1], makeunique=true, check = false)
        @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1], makeunique=true, check = false, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1], makeunique=true, check = false)
        @test sort(outerjoin(view(dsl, l_ridx, l_cidx), dsr, on =[:x1], makeunique=true, check = false, method = :hash), :) == sort(outerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on =[:x1], makeunique=true, check = false), :)
    end

    dsl = Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0])
    dsr = Dataset(x1 = [320, 250, 260, 120], y = [1,2,3,4])
    @test closejoin(dsl, dsr, on = :x1) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    dsl = Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0])
    dsr = Dataset(x1 = PooledArray([320, 250, 260, 120]), y = [1,2,3,4])
    @test closejoin(dsl, dsr, on = :x1) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    dsl = Dataset(x1 = PooledArray([100, 200, 300]), x2 = [5.0, 6.0, 7.0])
    dsr = Dataset(x1 = PooledArray([320, 250, 260, 120]), y = [1,2,3,4])
    @test closejoin(dsl, dsr, on = :x1) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = [100, 200, 300], x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    dsl = Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0])
    dsr = Dataset(x1 = Date.([320, 250, 260, 120]), y = [1,2,3,4])
    @test closejoin(dsl, dsr, on = :x1) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])

    @test closejoin(dsl, dsr, on = :x1, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])


    dsl = Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0])

    dsr = Dataset(x1 = DATE.([missing, 250, 260, 120]), y = [1,2,3,4])
    @test closejoin(dsl, dsr, on = :x1) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])

    @test closejoin(dsl, dsr, on = :x1, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])

    @test closejoin(dsl, dsr, on = :x1, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 4, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = Date.([100, 200, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 2, 3])

    dsl = Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0])
    dsr = Dataset(x1 = DATE.([missing, 250, 260, 120]), y = [1,2,3,4])
    @test closejoin(dsl, dsr, on = :x1) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])

    @test closejoin(dsl, dsr, on = :x1, method = :hash) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])

    @test closejoin(dsl, dsr, on = :x1, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin(dsl, dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, method = :hash, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [missing, 1, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 1])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])
    @test closejoin!(copy(dsl), dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1 = DATE.([100, missing, 300]), x2 = [5.0, 6.0, 7.0], y = [4, 1, 3])


    dsl = Dataset(x1 = [.3,.74,.53,.30, .65, 1])
    dsr = Dataset(x1 = [.31,.97,.6,.34], y = [1,2,3,4])

    @test closejoin(dsl, dsr, on = :x1) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])

    @test closejoin(dsl, dsr, on = :x1, border = :nearest) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1, 3,4,1, 3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :nearest) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :nearest) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])

    @test closejoin(dsl, dsr, on = :x1, border = :none) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :none) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,2,3,missing,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :none) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,3,3,missing,3, missing])

    @test closejoin(dsl, dsr, on = :x1, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])

    @test closejoin(dsl, dsr, on = :x1, border = :nearest, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1, 3,4,1, 3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :nearest, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :nearest, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])

    @test closejoin(dsl, dsr, on = :x1, border = :none, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :none, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,2,3,missing,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :none, method = :hash) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,3,3,missing,3, missing])


    @test closejoin(dsl, dsr, on = :x1, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,2])
    @test closejoin(dsl, dsr, on = :x1, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])

    @test closejoin(dsl, dsr, on = :x1, border = :nearest, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1, 3,4,1, 3,2])
    @test closejoin(dsl, dsr, on = :x1, border = :nearest, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1, 3,4,1, 3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :nearest, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :nearest, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,2,3,1,2,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :nearest, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :nearest, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [1,3,3,1,3,2])

    @test closejoin(dsl, dsr, on = :x1, border = :none, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,missing])
    @test closejoin(dsl, dsr, on = :x1, border = :none, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing, 3,4,missing, 3,missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :none, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,2,3,missing,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :forward, border = :none, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,2,3,missing,2, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :none, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,3,3,missing,3, missing])
    @test closejoin(dsl, dsr, on = :x1, direction = :nearest, border = :none, method = :hash, threads = false) == Dataset(x1=[.3,.74,.53,.30, .65,1], y = [missing,3,3,missing,3, missing])

    dsl = Dataset(x1 = [1,2,3,4], y = [100,200,300,400])
    dsr = Dataset(x1 = [2,1,5,6], y1 = [-100,-200,-300,-400])
    out1 = outerjoin(dsl, dsr, on = :x1, source = true)
    out2 = outerjoin(dsr, dsl, on = :x1, source = true)
    out1_t = Dataset(AbstractVector[Union{Missing, Int64}[1, 2, 3, 4, 5, 6], Union{Missing, Int64}[100, 200, 300, 400, missing, missing], Union{Missing, Int64}[-200, -100, missing, missing, -300, -400], Union{Missing, String}["both", "both", "left", "left", "right", "right"]], ["x1", "y", "y1", "source"])
    out2_t = Dataset(AbstractVector[Union{Missing, Int64}[2, 1, 5, 6, 3, 4], Union{Missing, Int64}[-100, -200, -300, -400, missing, missing], Union{Missing, Int64}[200, 100, missing, missing, 300, 400], Union{Missing, String}["both", "both", "left", "left", "right", "right"]], ["x1", "y1", "y", "source"])
    @test out1 == out1_t
    @test out2 == out2_t
    dsl = Dataset(x1 = [1,2,3,4], y = [100,200,300,400])
    dsr = Dataset(x1 = [2,1], y1 = [-100,missing])
    out1 = outerjoin(dsl, view(dsr,[2,1],:), on = :x1, source = true)
    out2 = outerjoin(view(dsr,[2,1],:), dsl, on = :x1, source = true)
    out1_t = Dataset(AbstractVector[Union{Missing, Int64}[1, 2, 3, 4], Union{Missing, Int64}[100, 200, 300, 400], Union{Missing, Int64}[missing, -100, missing, missing], Union{Missing, String}["both", "both", "left", "left"]], ["x1", "y", "y1", "source"])
    out2_t = Dataset(AbstractVector[Union{Missing, Int64}[1, 2, 3, 4], Union{Missing, Int64}[missing, -100, missing, missing], Union{Missing, Int64}[100, 200, 300, 400], Union{Missing, String}["both", "both", "right", "right"]], ["x1", "y1", "y", "source"])
    @test out1 == out1_t
    @test out2 == out2_t
    dsl = Dataset(x1 = [1,2,3,4],x2=[1,1,1,1], y = [100,200,300,400])
    dsr = Dataset(x1 = [2,1,5,6],x2= [1,1,1,1], y1 = [-100,-200,-300,-400])
    out1 = outerjoin(dsl, dsr, on = [:x1, :x2], source = true)
    out2 = outerjoin(dsr, dsl, on = [:x1, :x2], source = true)
    out1_t = Dataset(AbstractVector[Union{Missing, Int64}[1, 2, 3, 4, 5, 6], Union{Missing, Int64}[1, 1, 1, 1, 1, 1], Union{Missing, Int64}[100, 200, 300, 400, missing, missing], Union{Missing, Int64}[-200, -100, missing, missing, -300, -400], Union{Missing, String}["both", "both", "left", "left", "right", "right"]], ["x1", "x2", "y", "y1", "source"])
    out2_t = Dataset(AbstractVector[Union{Missing, Int64}[2, 1, 5, 6, 3, 4], Union{Missing, Int64}[1, 1, 1, 1, 1, 1], Union{Missing, Int64}[-100, -200, -300, -400, missing, missing], Union{Missing, Int64}[200, 100, missing, missing, 300, 400], Union{Missing, String}["both", "both", "left", "left", "right", "right"]], ["x1", "x2", "y1", "y", "source"])
    @test out1 == out1_t
    @test out2 == out2_t

end

@testset "Test empty inputs 1" begin
    simple_ds(len::Int, col=:A) = (ds = Dataset();
                                   ds[!, col]=Vector{Union{Int, Missing}}(1:len);
                                   ds)
    @test leftjoin(simple_ds(0), simple_ds(0), on = :A) == simple_ds(0)
    @test leftjoin(simple_ds(2), simple_ds(0), on = :A) == simple_ds(2)
    @test leftjoin(simple_ds(0), simple_ds(2), on = :A) == simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(0), on = :A) == simple_ds(0)
    @test semijoin(simple_ds(2), simple_ds(0), on = :A) == simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(2), on = :A) == simple_ds(0)
    @test antijoin(simple_ds(0), simple_ds(0), on = :A) == simple_ds(0)
    @test antijoin(simple_ds(2), simple_ds(0), on = :A) == simple_ds(2)
    @test antijoin(simple_ds(0), simple_ds(2), on = :A) == simple_ds(0)

    @test leftjoin(simple_ds(0), simple_ds(0), on = :A, method = :hash) == simple_ds(0)
    @test leftjoin(simple_ds(2), simple_ds(0), on = :A, method = :hash) == simple_ds(2)
    @test leftjoin(simple_ds(0), simple_ds(2), on = :A, method = :hash) == simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(0), on = :A, method = :hash) == simple_ds(0)
    @test semijoin(simple_ds(2), simple_ds(0), on = :A, method = :hash) == simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(2), on = :A, method = :hash) == simple_ds(0)
    @test antijoin(simple_ds(0), simple_ds(0), on = :A, method = :hash) == simple_ds(0)
    @test antijoin(simple_ds(2), simple_ds(0), on = :A, method = :hash) == simple_ds(2)
    @test antijoin(simple_ds(0), simple_ds(2), on = :A, method = :hash) == simple_ds(0)
end

@testset "Test empty inputs 2" begin
    simple_ds(len::Int, col=:A) = (ds = Dataset(); ds[!, col]=collect(1:len); ds)
    @test leftjoin(simple_ds(0), simple_ds(0), on = :A) ==  simple_ds(0)
    @test leftjoin(simple_ds(2), simple_ds(0), on = :A) ==  simple_ds(2)
    @test leftjoin(simple_ds(0), simple_ds(2), on = :A) ==  simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(0), on = :A) ==  simple_ds(0)
    @test semijoin(simple_ds(2), simple_ds(0), on = :A) ==  simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(2), on = :A) ==  simple_ds(0)
    @test antijoin(simple_ds(0), simple_ds(0), on = :A) ==  simple_ds(0)
    @test antijoin(simple_ds(2), simple_ds(0), on = :A) ==  simple_ds(2)
    @test antijoin(simple_ds(0), simple_ds(2), on = :A) ==  simple_ds(0)

    @test leftjoin(simple_ds(0), simple_ds(0), on = :A, method = :hash) ==  simple_ds(0)
    @test leftjoin(simple_ds(2), simple_ds(0), on = :A, method = :hash) ==  simple_ds(2)
    @test leftjoin(simple_ds(0), simple_ds(2), on = :A, method = :hash) ==  simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(0), on = :A, method = :hash) ==  simple_ds(0)
    @test semijoin(simple_ds(2), simple_ds(0), on = :A, method = :hash) ==  simple_ds(0)
    @test semijoin(simple_ds(0), simple_ds(2), on = :A, method = :hash) ==  simple_ds(0)
    @test antijoin(simple_ds(0), simple_ds(0), on = :A, method = :hash) ==  simple_ds(0)
    @test antijoin(simple_ds(2), simple_ds(0), on = :A, method = :hash) ==  simple_ds(2)
    @test antijoin(simple_ds(0), simple_ds(2), on = :A, method = :hash) ==  simple_ds(0)

end

@testset "all joins" begin
    ds1 = Dataset(A = categorical(1:50),
                    B = categorical(1:50),
                    C = 1)
    @test innerjoin(ds1, ds1, on = [:A, :B], makeunique=true)[!, 1:3] == ds1
    @test innerjoin(ds1, ds1, on = [:A, :B], makeunique=true, accelerate = true)[!, 1:3] == ds1

    @test innerjoin(ds1, ds1, on = [:A, :B], method = :hash, makeunique=true)[!, 1:3] == ds1
    @test innerjoin(ds1, ds1, on = [:A, :B], method = :hash, makeunique=true, accelerate = true)[!, 1:3] == ds1
    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1088)
    ds = Dataset(Name = Union{String, Missing}["A", "B", "C"],
                Mass = [1.5, 2.2, 1.1])
    ds2 = Dataset(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test leftjoin(ds2, ds, on=:Name) == Dataset(Name = ["A", "B", "C", "A"],
                                                   Quantity = [3, 3, 2, 4],
                                                   Mass = [1.5, 2.2, 1.1, 1.5])
   @test leftjoin(ds2, ds, on=:Name, method = :hash) == Dataset(Name = ["A", "B", "C", "A"],
                                                  Quantity = [3, 3, 2, 4],
                                                  Mass = [1.5, 2.2, 1.1, 1.5])

    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1151)
    ds = Dataset([collect(1:10), collect(2:11)], [:x, :y])
    dsmissing = Dataset(x = Vector{Union{Int, Missing}}(1:10),
                        z = Vector{Union{Int, Missing}}(3:12))
    @test innerjoin(ds, dsmissing, on = :x) ==
        Dataset([collect(1:10), collect(2:11), collect(3:12)], [:x, :y, :z])
    @test innerjoin(dsmissing, ds, on = :x) ==
        Dataset([Vector{Union{Int, Missing}}(1:10), Vector{Union{Int, Missing}}(3:12),
                collect(2:11)], [:x, :z, :y])
    @test innerjoin(ds, dsmissing, on = :x, method = :hash) ==
        Dataset([collect(1:10), collect(2:11), collect(3:12)], [:x, :y, :z])
    @test innerjoin(dsmissing, ds, on = :x, method = :hash) ==
        Dataset([Vector{Union{Int, Missing}}(1:10), Vector{Union{Int, Missing}}(3:12),
                collect(2:11)], [:x, :z, :y])
    ds1 = Dataset(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
    ds2 = Dataset(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])


    i(on) = innerjoin(ds1, ds2, on = on, makeunique=true)
    l(on) = leftjoin(ds1, ds2, on = on, makeunique=true)
    o(on) = outerjoin(ds1, ds2, on = on, makeunique=true)
    s(on) = semijoin(ds1, ds2, on = on)
    a(on) = antijoin(ds1, ds2, on = on)

    @test s(:id) ==
          s(:fid) ==
          s([:id, :fid]) == Dataset([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(s(:id))) ==
          typeof.(eachcol(s(:fid))) ==
          typeof.(eachcol(s([:id, :fid]))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]
    @test a(:id) ==
          a(:fid) ==
          a([:id, :fid]) == Dataset([[5], [5]], [:id, :fid])
    @test typeof.(eachcol(a(:id))) ==
          typeof.(eachcol(a(:fid))) ==
          typeof.(eachcol(a([:id, :fid]))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]

    on = :id
    @test i(on) == Dataset([[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
    @test typeof.(eachcol(i(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}, Vector{Union{Missing, Float64}}]
    @test l(on) ≅ Dataset(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            fid_1 = [1, 3, missing])
    @test typeof.(eachcol(l(on))) ==
        [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}, Vector{Union{Float64, Missing}}]


    @test o(on) ≅ Dataset(id = [1, 3, 5, 0, 2, 4],
                            fid = [1, 3, 5, missing, missing, missing],
                            fid_1 = [1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o(on))) ==
        [Vector{Union{Missing, Int}}, Vector{Union{Float64, Missing}}, Vector{Union{Float64, Missing}}]

    on = :fid
    @test i(on) == Dataset([[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
    @test typeof.(eachcol(i(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}, Vector{Union{Missing, Int}}]
    @test l(on) ≅ Dataset(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            id_1 = [1, 3, missing])
    @test typeof.(eachcol(l(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}},
                                     Vector{Union{Int, Missing}}]

    @test o(on) ≅ Dataset(id = [1, 3, 5, missing, missing, missing],
                            fid = [1, 3, 5, 0, 2, 4],
                            id_1 = [1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o(on))) == [Vector{Union{Int, Missing}}, Vector{Union{Missing, Float64}},
                                     Vector{Union{Int, Missing}}]

    on = [:id, :fid]
    @test i(on) == Dataset([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(i(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]
    @test l(on) == Dataset(id = [1, 3, 5], fid = [1, 3, 5])
    @test typeof.(eachcol(l(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]

    @test o(on) == Dataset(id = [1, 3, 5, 0, 2, 4], fid = [1, 3, 5, 0, 2, 4])
    @test typeof.(eachcol(o(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]


    i_hash(on) = innerjoin(ds1, ds2, on = on, makeunique=true, method = :hash)
    l_hash(on) = leftjoin(ds1, ds2, on = on, makeunique=true, method = :hash)
    o_hash(on) = outerjoin(ds1, ds2, on = on, makeunique=true, method = :hash)
    s_hash(on) = semijoin(ds1, ds2, on = on, method = :hash)
    a_hash(on) = antijoin(ds1, ds2, on = on, method = :hash)

    @test s_hash(:id) ==
          s_hash(:fid) ==
          s_hash([:id, :fid]) == Dataset([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(s_hash(:id))) ==
          typeof.(eachcol(s_hash(:fid))) ==
          typeof.(eachcol(s_hash([:id, :fid]))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]
    @test a_hash(:id) ==
          a_hash(:fid) ==
          a_hash([:id, :fid]) == Dataset([[5], [5]], [:id, :fid])
    @test typeof.(eachcol(a_hash(:id))) ==
          typeof.(eachcol(a_hash(:fid))) ==
          typeof.(eachcol(a_hash([:id, :fid]))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]

    on = :id
    @test i_hash(on) == Dataset([[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
    @test typeof.(eachcol(i_hash(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}, Vector{Union{Missing, Float64}}]
    @test l_hash(on) ≅ Dataset(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            fid_1 = [1, 3, missing])
    @test typeof.(eachcol(l_hash(on))) ==
        [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}, Vector{Union{Float64, Missing}}]


    @test o_hash(on) ≅ Dataset(id = [1, 3, 5, 0, 2, 4],
                            fid = [1, 3, 5, missing, missing, missing],
                            fid_1 = [1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o_hash(on))) ==
        [Vector{Union{Missing, Int}}, Vector{Union{Float64, Missing}}, Vector{Union{Float64, Missing}}]

    on = :fid
    @test i_hash(on) == Dataset([[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
    @test typeof.(eachcol(i_hash(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}, Vector{Union{Missing, Int}}]
    @test l_hash(on) ≅ Dataset(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            id_1 = [1, 3, missing])
    @test typeof.(eachcol(l_hash(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}},
                                     Vector{Union{Int, Missing}}]

    @test o_hash(on) ≅ Dataset(id = [1, 3, 5, missing, missing, missing],
                            fid = [1, 3, 5, 0, 2, 4],
                            id_1 = [1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o_hash(on))) == [Vector{Union{Int, Missing}}, Vector{Union{Missing, Float64}},
                                     Vector{Union{Int, Missing}}]

    on = [:id, :fid]
    @test i_hash(on) == Dataset([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(i_hash(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]
    @test l_hash(on) == Dataset(id = [1, 3, 5], fid = [1, 3, 5])
    @test typeof.(eachcol(l_hash(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]

    @test o_hash(on) == Dataset(id = [1, 3, 5, 0, 2, 4], fid = [1, 3, 5, 0, 2, 4])
    @test typeof.(eachcol(o_hash(on))) == [Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}]

    #####
    dsl = Dataset(x=[1,2], y=[3,4])
    re = innerjoin(dsl, dsl, on = [:x=>:y], makeunique = true)
    @test re == innerjoin(dsl, dsl, on = [:x=>:y], makeunique = true, method = :hash)
    @test Dataset([[],[],[]], names(re)) == re


    dsl = Dataset(x1 = [1,2,3,4,5,6], x2= [1,1,1,2,2,2])
    dsr = Dataset(x1 = [1,1,1,4,5,7],x2= [1,1,3,4,5,6], y = [343,54,54,464,565,7567])
    cj = closejoin(dsl, dsr, on = [:x1, :x2])
    cj_v = closejoin(dsl, view(dsr, 1:6, [3,1,2]), on = [:x1, :x2])

    @test cj == closejoin(dsl, dsr, on = [:x1, :x2], method = :hash)
    @test cj_v == closejoin(dsl, view(dsr, 1:6, [3,1,2]), on = [:x1, :x2], method = :hash)

    cj_t = Dataset([Union{Missing, Int64}[1, 2, 3, 4, 5, 6],
         Union{Missing, Int64}[1, 1, 1, 2, 2, 2],
         Union{Missing, Int64}[54, missing, missing, missing, missing, missing]], ["x1", "x2", "y"])
    @test cj == cj_t
    @test cj_v == cj_t
    cj = closejoin(dsl, dsr, on = [:x1, :x2], direction = :forward)
    cj_v = closejoin(dsl, view(dsr, 1:6, [3,1,2]), on = [:x1, :x2], direction = :forward)

    @test cj == closejoin(dsl, dsr, on = [:x1, :x2], direction = :forward, method = :hash)
    @test cj_v == closejoin(dsl, view(dsr, 1:6, [3,1,2]), on = [:x1, :x2], direction = :forward, method = :hash)

    cj_t = Dataset([ Union{Missing, Int64}[1, 2, 3, 4, 5, 6],
         Union{Missing, Int64}[1, 1, 1, 2, 2, 2],
         Union{Missing, Int64}[343, missing, missing, 464, 565, missing]],["x1", "x2", "y"] )
    @test cj == cj_t
    @test cj_v == cj_t

    dsl = Dataset(x1 = [1,2,3,4,5,6], x2= [1,1,1,2,2,2])
    dsr = Dataset(x1 = [1,1,1,4,5],x2= [1,3,4,5,6], y = [343,54,54,464,565])
    fmt_close(x) = x < 3 ? 1 : x < 5 ? 2 : x
    setformat!(dsl, 1=>fmt_close)

    @test byrow(compare(closejoin(dsl, dsr, on = :x1=>:x2, makeunique = true),  Dataset(x1=[1,1,2,2,5,6], x2=[1,1,1,2,2,2], x1_1=[1,1,1,1,4,5], y=[343,343,343,343,464,565]), mapformats = true), all)|>all
    @test byrow(compare(closejoin(dsl, dsr, on = :x1=>:x2, makeunique = true, direction = :forward),  Dataset(x1=[1,1,2,2,5,6], x2=[1,1,1,2,2,2], x1_1=[1,1,1,1,4,5], y=[343,343,54,54,464,565]), mapformats = true), all)|>all

    @test byrow(compare(closejoin(dsl, dsr, on = :x1=>:x2, makeunique = true, method = :hash),  Dataset(x1=[1,1,2,2,5,6], x2=[1,1,1,2,2,2], x1_1=[1,1,1,1,4,5], y=[343,343,343,343,464,565]), mapformats = true), all)|>all
    @test byrow(compare(closejoin(dsl, dsr, on = :x1=>:x2, makeunique = true, method = :hash, direction = :forward),  Dataset(x1=[1,1,2,2,5,6], x2=[1,1,1,2,2,2], x1_1=[1,1,1,1,4,5], y=[343,343,54,54,464,565]), mapformats = true), all)|>all


    dsl = Dataset(x1 = [Date(2020,11,6), Date(2021,2,24), Date(2021,1,17), Date(2013,5,12)], val = [66,77,88,99])
    dsr = Dataset(x1 = [Date(2010,11,2), Date(2012, 5, 3), Date(2010, 2,2)], x2 = [1,2,3])
    setformat!(dsl, 1=>month)
    setformat!(dsr, 1=>month)
    out_l1 = leftjoin(dsl, dsr, on = :x1, mapformats = false)
    out_l2 = leftjoin(dsl, dsr, on = :x1, mapformats = true)

    @test out_l1 == leftjoin(dsl, dsr, on = :x1, mapformats = false, method = :hash)
    @test out_l2 == leftjoin(dsl, dsr, on = :x1, mapformats = true, method = :hash)

    out_t1 = Dataset([Union{Missing, Date}[Date("2020-11-06"), Date("2021-02-24"), Date("2021-01-17"), Date("2013-05-12")],
             Union{Missing, Int64}[66, 77, 88, 99],
             Union{Missing, Int64}[missing, missing, missing, missing]], [:x1, :val, :x2])
    out_t2 = Dataset([Union{Missing, Date}[Date("2020-11-06"), Date("2021-02-24"), Date("2021-01-17"), Date("2013-05-12")],
             Union{Missing, Int64}[66, 77, 88, 99],
             Union{Missing, Int64}[1, 3, missing, 2]], [:x1, :val, :x2])
    @test out_l1 == out_t1
    @test out_l2 == out_t2
    dsl = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
         Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
         Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
         Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], ["x1", "x2", "x3", "row"])
    dsr = Dataset(x1=[1, 3, 2], y =[100.0, 200.0, 300.0])
    setformat!(dsr, 1=>isodd)

    left1 = leftjoin(dsl, dsr, on = :x1, mapformats = false)
    @test left1 == leftjoin(dsl, dsr, on = :x1, mapformats = false, method = :hash)

    left1_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
             Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
             Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
             Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
             Union{Missing, Float64}[missing, 200.0, missing, 100.0, missing, missing, missing, missing, 300.0, missing]], ["x1", "x2", "x3", "row", "y"])
    @test left1 == left1_t


    A = Dataset(a = [1, 2, 3], b = ["a", "b", "c"])
    B = Dataset(b = ["a", "b", "c"], c = CategoricalVector(["a", "b", "b"]))
    levels!(B.c.val, ["b", "a"])
    @test levels(innerjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(innerjoin(B, A, on=:b).c) == ["b", "a"]
    @test levels(leftjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(outerjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(semijoin(B, A, on=:b).c) == ["b", "a"]

    @test levels(innerjoin(A, B, on=:b, method = :hash).c) == ["b", "a"]
    @test levels(innerjoin(B, A, on=:b, method = :hash).c) == ["b", "a"]
    @test levels(leftjoin(A, B, on=:b, method = :hash).c) == ["b", "a"]
    @test levels(outerjoin(A, B, on=:b, method = :hash).c) == ["b", "a"]
    @test levels(semijoin(B, A, on=:b, method = :hash).c) == ["b", "a"]

    @test levels(innerjoin(A, view(B, [3,2,1], [2,1]), on=:b).c) == ["b", "a"]
    @test levels(innerjoin(B,  view(A, [3,2,1], [2,1]), on=:b).c) == ["b", "a"]
    @test levels(leftjoin(A,  view(B, [3,2,1], [2,1]), on=:b).c) == ["b", "a"]
    @test levels(outerjoin(A,  view(B, [3,2,1], [2,1]), on=:b).c) == ["b", "a"]
    @test levels(semijoin(B,  view(A, [3,2,1], [2,1]), on=:b).c) == ["b", "a"]

    @test levels(innerjoin(A, view(B, [3,2,1], [2,1]), on=:b, method = :hash).c) == ["b", "a"]
    @test levels(innerjoin(B,  view(A, [3,2,1], [2,1]), on=:b, method = :hash).c) == ["b", "a"]
    @test levels(leftjoin(A,  view(B, [3,2,1], [2,1]), on=:b, method = :hash).c) == ["b", "a"]
    @test levels(outerjoin(A,  view(B, [3,2,1], [2,1]), on=:b, method = :hash).c) == ["b", "a"]
    @test levels(semijoin(B,  view(A, [3,2,1], [2,1]), on=:b, method = :hash).c) == ["b", "a"]

    dsl = Dataset(x = categorical(["c","d",missing, "e","c"]), y = 1:5)
    dsr = Dataset(x = categorical(["a", "f", "e", "c"]), z = PooledArray([22,missing,33,44]))
    ds_left = leftjoin(dsl, dsr, on = :x, stable = true)
    @test ds_left == leftjoin(dsl, dsr, on = :x, method = :hash)
    ds_left_t = Dataset([categorical(["c", "d", missing, "e", "c"]),
                 Union{Missing, Int64}[1, 2, 3, 4, 5],
                 Union{Missing, Int64}[44, missing, missing, 33, 44]],[:x, :y, :z])
    @test ds_left == ds_left_t
    ds_left = leftjoin(dsr, dsl, on = :x, stable = true)
    @test ds_left == leftjoin(dsr, dsl, on = :x, method = :hash)
    ds_left_t = Dataset([categorical(["a", "f", "e", "c", "c"]),
                 Union{Missing, Int64}[22, missing, 33, 44, 44],
                 Union{Missing, Int64}[missing, missing, 4, 1, 5]],[:x, :z, :y])
    ds_inner = innerjoin(dsl, dsr, on = :x, stable = true)
    @test ds_inner == innerjoin(dsl, dsr, on = :x, method = :hash)

    ds_inner_t = Dataset([categorical(["c", "e", "c"]),
                 Union{Missing, Int64}[1, 4, 5],
                 Union{Missing, Int64}[44, 33, 44]], [:x, :y, :z])
    @test ds_inner == ds_inner_t
    for i in 1:20 # when we fix the issue with Threads we can make sure it is ok
        ds_outer = outerjoin(dsl, dsr, on = :x, stable = true)
        @test ds_outer == outerjoin(dsl, dsr, on = :x, method = :hash)
        ds_outer_t = Dataset([categorical(["c", "d", missing, "e", "c", "a", "f"]),
                 Union{Missing, Int64}[1, 2, 3, 4, 5, missing, missing],
                 Union{Missing, Int64}[44, missing, missing, 33, 44, 22, missing]], [:x, :y, :z])
        @test ds_outer == ds_outer_t
    end
    dsl = Dataset(x = categorical(["c","d",missing, "e","c"]), y = 1:5)
    dsr = Dataset(x = categorical(["a", "f", "e", "c"]), z = PooledArray([2,missing,3,4]))
    for i in 1:20
        ds_left = leftjoin(dsl, dsr, on = [:y=>:z], makeunique=true, stable = true)
        @test ds_left == leftjoin(dsl, dsr, on = [:y=>:z], makeunique=true, method = :hash)
        ds_left_t = Dataset([categorical(["c", "d", missing, "e", "c"]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5],
                     categorical([missing, "a", "e", "c", missing])],[:x, :y, :x_1])
        @test ds_left == ds_left_t
        ds_outer = outerjoin(dsl, dsr, on = [:y=>:z], makeunique=true, stable = true)
        @test ds_outer == outerjoin(dsl, dsr, on = [:y=>:z], makeunique=true, method = :hash)
        ds_outer_t = Dataset([ categorical(["c", "d", missing, "e", "c", missing]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5, missing],
                     categorical([missing, "a", "e", "c", missing, "f"])], [:x, :y, :x_1])
        @test ds_outer == ds_outer_t
    end
    for i in 1:20
        ds_left = leftjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, stable = true)
        @test ds_left == leftjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, method = :hash)
        ds_left_t = Dataset([categorical(["c", "d", missing, "e", "c"]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5],
                     categorical([missing, "a", "e", "c", missing])],[:x, :y, :x_1])
        @test ds_left == ds_left_t
        ds_outer = outerjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, stable = true)
        @test ds_outer == outerjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, method = :hash)
        ds_outer_t = Dataset([ categorical(["c", "d", missing, "e", "c", missing]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5, missing],
                     categorical([missing, "a", "e", "c", missing, "f"])], [:x, :y, :x_1])
        @test ds_outer == ds_outer_t
    end
    dsl = Dataset(x = categorical(["c","d",missing, "e","c"]), y = PooledArray(1:5))
    dsr = Dataset(x = categorical(["a", "f", "e", "c"]), z = PooledArray([2,missing,3,4]))
    for i in 1:20
        ds_left = leftjoin(dsl, dsr, on = [:y=>:z], makeunique=true, stable = true)
        @test ds_left == leftjoin(dsl, dsr, on = [:y=>:z], makeunique=true, method = :hash)
        ds_left_t = Dataset([categorical(["c", "d", missing, "e", "c"]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5],
                    categorical([missing, "a", "e", "c", missing])],[:x, :y, :x_1])
        @test ds_left == ds_left_t
        ds_outer = outerjoin(dsl, dsr, on = [:y=>:z], makeunique=true, stable = true)
        @test ds_outer == outerjoin(dsl, dsr, on = [:y=>:z], makeunique=true, method = :hash)
        ds_outer_t = Dataset([ categorical(["c", "d", missing, "e", "c", missing]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5, missing],
                     categorical([missing, "a", "e", "c", missing, "f"])], [:x, :y, :x_1])
        @test ds_outer == ds_outer_t
    end
    for i in 1:20
        ds_left = leftjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, stable = true)
        @test ds_left == leftjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, method = :hash)
        ds_left_t = Dataset([categorical(["c", "d", missing, "e", "c"]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5],
                    categorical([missing, "a", "e", "c", missing])],[:x, :y, :x_1])
        @test ds_left == ds_left_t
        ds_outer = outerjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, stable = true)
        @test ds_outer == outerjoin(dsl, view(dsr, :, :), on = [:y=>:z], makeunique=true, method = :hash)
        ds_outer_t = Dataset([ categorical(["c", "d", missing, "e", "c", missing]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5, missing],
                     categorical([missing, "a", "e", "c", missing, "f"])], [:x, :y, :x_1])
        @test ds_outer == ds_outer_t
    end
    dsl = Dataset(x = categorical(["c","d",missing, "e","c"]), y = PooledArray(1:5))
    dsr = Dataset(x = categorical(["a", "f", "e", "c"]), z = [2,missing,3,4])
    for i in 1:20
        ds_left = leftjoin(dsl, dsr, on = [:y=>:z], makeunique=true, stable = true)
        @test ds_left == leftjoin(dsl, dsr, on = [:y=>:z], makeunique=true, method = :hash)
        ds_left_t = Dataset([categorical(["c", "d", missing, "e", "c"]),
                     [1, 2, 3, 4, 5],
                     categorical([missing, "a", "e", "c", missing])],[:x, :y, :x_1])
        @test ds_left == ds_left_t
        ds_outer = outerjoin(dsl, dsr, on = [:y=>:z], makeunique=true, stable = true)
        @test ds_outer == outerjoin(dsl, dsr, on = [:y=>:z], makeunique=true, method = :hash)
        ds_outer_t = Dataset([categorical(["c", "d", missing, "e", "c", missing]),
                     Union{Missing, Int64}[1, 2, 3, 4, 5, missing],
                     categorical([missing, "a", "e", "c", missing, "f"])], [:x, :y, :x_1])
        @test ds_outer == ds_outer_t
    end

    dsl = Dataset(x = PooledArray([1, 7, 19, missing]), y = 1:4)
    dsr = Dataset(x = [missing,5, 19, 1], z = ["a", "b", "c", "d"])
    for i in 1:20
        res = contains(dsl, dsr, on = :x)
        @test res == contains(dsl, dsr, on = :x, method = :hash)
        @test res == Bool[1,0,1,1]
    end
    for i in 1:20
        res = contains(dsl, dsr, on = :x, accelerate = true)
        @test res == Bool[1,0,1,1]
    end
    for i in 1:20
        res = contains(dsr, dsl, on = :x)
        @test res == contains(dsr, dsl, on = :x, method = :hash)
        @test res == Bool[1,0,1,1]
    end
    dsl = Dataset(x = PooledArray([1, 7, 19, missing]), y = 1:4)
    dsr = Dataset(x = categorical([missing,5, 19, 1]), z = ["a", "b", "c", "d"])
    for i in 1:20
        res = contains(dsl, dsr, on = :x)
        @test res == contains(dsl, dsr, on = :x, method = :hash)
        @test res == Bool[1,0,1,1]
    end
    for i in 1:20
        res = contains(dsr, dsl, on = :x)
        @test res == contains(dsr, dsl, on = :x, method = :hash)
        @test res == Bool[1,0,1,1]
    end
    dsl = Dataset(x = categorical([1, 7, 19, missing]), y = 1:4)
    dsr = Dataset(x = categorical([missing,5, 19, 1]), z = ["a", "b", "c", "d"])
    for i in 1:20
        res = contains(dsl, dsr, on = :x)
        @test res == contains(dsl, dsr, on = :x, method = :hash)
        @test res == Bool[1,0,1,1]
    end
    for i in 1:20
        res = contains(dsr, dsl, on = :x)
        @test res == contains(dsr, dsl, on = :x, method = :hash)
        @test res == Bool[1,0,1,1]
    end

end



@testset "joins with categorical columns and no matching rows - from DataFrames test sets" begin
    l = Dataset(a=1:3, b=categorical(["a", "b", "c"]))
    r = Dataset(a=4:5, b=categorical(["d", "e"]))
    nl = size(l, 1)
    nr = size(r, 1)

    CS = eltype(l.b.val)

    # joins by a and b
    @test innerjoin(l, r, on=[:a, :b]) == Dataset(a=Int[], b=similar(l.a.val, 0))
    @test innerjoin(l, r, on=[:a, :b], method = :hash) == Dataset(a=Int[], b=similar(l.a.val, 0))
    @test eltype.(eachcol(innerjoin(l, r, on=[:a, :b]))) == [Union{Missing, Int}, CS]

    @test leftjoin(l, r, on=[:a, :b]) == Dataset(a=l.a.val, b=l.b.val)
    @test leftjoin(l, r, on=[:a, :b], method = :hash) == Dataset(a=l.a.val, b=l.b.val)
    @test eltype.(eachcol(leftjoin(l, r, on=[:a, :b]))) == [Union{Int, Missing}, CS]

    @test outerjoin(l, r, on=[:a, :b]) ==
        Dataset(a=vcat(l.a.val, r.a.val), b=vcat(l.b.val, r.b.val))
    @test outerjoin(l, r, on=[:a, :b], method = :hash) ==
        Dataset(a=vcat(l.a.val, r.a.val), b=vcat(l.b.val, r.b.val))
    @test eltype.(eachcol(outerjoin(l, r, on=[:a, :b]))) == [Union{Int, Missing}, CS]

    # joins by a
    @test innerjoin(l, r, on=:a, makeunique=true) ==
        Dataset(a=Int[], b=similar(l.b.val, 0), b_1=similar(r.b.val, 0))
    @test innerjoin(l, r, on=:a, makeunique=true, method = :hash) ==
        Dataset(a=Int[], b=similar(l.b.val, 0), b_1=similar(r.b.val, 0))
    @test eltype.(eachcol(innerjoin(l, r, on=:a, makeunique=true))) == [Union{Missing, Int}, CS, CS]

    @test leftjoin(l, r, on=:a, makeunique=true) ==
        Dataset(a=l.a.val, b=l.b.val, b_1=similar(r.b.val, nl))
    @test leftjoin(l, r, on=:a, makeunique=true, method = :hash) ==
        Dataset(a=l.a.val, b=l.b.val, b_1=similar(r.b.val, nl))
    @test eltype.(eachcol(leftjoin(l, r, on=:a, makeunique=true))) ==
        [Union{Missing, Int}, CS, Union{CS, Missing}]

    @test outerjoin(l, r, on=:a, makeunique=true) ==
        Dataset(a=vcat(l.a.val, r.a.val),
                  b=vcat(l.b.val, fill(missing, nr)),
                  b_1=vcat(fill(missing, nl), r.b.val))
    @test outerjoin(l, r, on=:a, makeunique=true, method = :hash) ==
        Dataset(a=vcat(l.a.val, r.a.val),
                  b=vcat(l.b.val, fill(missing, nr)),
                  b_1=vcat(fill(missing, nl), r.b.val))
    @test eltype.(eachcol(outerjoin(l, r, on=:a, makeunique=true))) ==
        [Union{Missing, Int}, Union{CS, Missing}, Union{CS, Missing}]

    # joins by b
    @test innerjoin(l, r, on=:b, makeunique=true) ==
        Dataset(a=Int[], b=similar(l.b.val, 0), a_1=similar(r.b.val, 0))
    @test innerjoin(l, r, on=:b, makeunique=true, method = :hash) ==
        Dataset(a=Int[], b=similar(l.b.val, 0), a_1=similar(r.b.val, 0))
    @test eltype.(eachcol(innerjoin(l, r, on=:b, makeunique=true))) == [Union{Missing, Int}, CS, Union{Missing, Int}]

    @test leftjoin(l, r, on=:b, makeunique=true) ==
        Dataset(a=l.a.val, b=l.b.val, a_1=fill(missing, nl))
    @test leftjoin(l, r, on=:b, makeunique=true, method = :hash) ==
        Dataset(a=l.a.val, b=l.b.val, a_1=fill(missing, nl))
    @test eltype.(eachcol(leftjoin(l, r, on=:b, makeunique=true))) ==
        [Union{Missing, Int}, CS, Union{Int, Missing}]

    @test outerjoin(l, r, on=:b, makeunique=true) ==
        Dataset(a=vcat(l.a.val, fill(missing, nr)),
                  b=vcat(l.b.val, r.b.val),
                  a_1=vcat(fill(missing, nl), r.a.val))
  @test outerjoin(l, r, on=:b, makeunique=true, method = :hash) ==
      Dataset(a=vcat(l.a.val, fill(missing, nr)),
                b=vcat(l.b.val, r.b.val),
                a_1=vcat(fill(missing, nl), r.a.val))
    @test eltype.(eachcol(outerjoin(l, r, on=:b, makeunique=true))) ==
        [Union{Int, Missing}, CS, Union{Int, Missing}]
    # joining categorical column with non-ca one
    dsl = Dataset(x= categorical([1,2,1]))
    dsr = Dataset(x=1:5)
    @test leftjoin(dsl, dsr, on =:x) == dsl
    @test deleteat!(leftjoin(dsr, dsl, on =:x), 1) == dsr

    dsl = Dataset(x= categorical([1,2,1]))
    dsr = Dataset(x=1:5)
    @test leftjoin(dsl, dsr, on =:x, method = :hash) == dsl
    @test deleteat!(leftjoin(dsr, dsl, on =:x, method = :hash), 1) == dsr
end

@testset "range join" begin
    store = Dataset([[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-02"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03")],
        ["A", "A", "B", "A", "B", "A", "B", "B", "A", "B"]], [:date, :store])
    roster = Dataset([Union{Missing, String}["A", "A", "B", "A", "B", "A", "B", "B"],
             Union{Missing, Int64}[4, 1, 8, 2, 5, 3, 6, 7],
             Union{Missing, Date}[Date("2019-10-04"), Date("2019-09-30"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-09-30"), Date("2019-10-03"), Date("2019-10-02"), Date("2019-10-03")],
             Union{Missing, Date}[Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-05")]],
             ["store", "employee_ID", "start_date", "end_date"])
    inn_r1 =  innerjoin(store, roster, on = [:date => (:start_date, nothing)], makeunique = true, stable = true)
    inn_r1_v =  innerjoin(store, view(roster, :, :), on = [:date => (:start_date, nothing)], makeunique = true, stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:date => (:start_date, nothing)], makeunique = true, stable = true, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, :), on = [:date => (:start_date, nothing)], makeunique = true, stable = true, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:date => (:start_date, nothing)], makeunique = true, stable = true, method = :hash)
    @test inn_r1_v ==  innerjoin(store, view(roster, :, :), on = [:date => (:start_date, nothing)], makeunique = true, stable = true, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:date => (:start_date, nothing)], makeunique = true, stable = true, accelerate = true, method = :hash)
    @test inn_r1_v_a ==  innerjoin(store, view(roster, :, :), on = [:date => (:start_date, nothing)], makeunique = true, stable = true, accelerate = true, method = :hash)


    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "A", "A", "A", "A", "A", "A", "A", "A", "B", "B", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B", "A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B"], Union{Missing, String}["A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B", "A", "B"], Union{Missing, Int64}[1, 5, 2, 6, 3, 7, 4, 8, 1, 5, 2, 6, 3, 7, 4, 8, 1, 5, 2, 6, 1, 5, 2, 6, 3, 7, 4, 8, 1, 5, 1, 5, 2, 6, 1, 5, 2, 6, 3, 7, 4, 8, 1, 5, 2, 6, 3, 7, 4, 8, 1, 5, 2, 6, 3, 7, 1, 5, 2, 6, 3, 7], Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-05")]], ["date", "store", "store_1", "employee_ID", "end_date"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_v == inn_r1_t
    @test inn_r1_a == inn_r1_t
    @test inn_r1_v_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, nothing)], stable = true)
    inn_r1_v =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, nothing)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, nothing)], stable = true, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, nothing)], stable = true, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, nothing)], stable = true, method = :hash)
    @test inn_r1_v ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, nothing)], stable = true, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, nothing)], stable = true, accelerate = true, method = :hash)
    @test inn_r1_v_a ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, nothing)], stable = true, accelerate = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "A", "A", "A", "A", "A", "B", "B", "A", "A", "A", "A", "B", "A", "A", "B", "B", "B", "B", "B", "B", "B", "B", "A", "A", "A", "B", "B", "B"], Union{Missing, Int64}[1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 1, 2, 5, 6, 7, 8, 5, 6, 7, 8, 1, 2, 3, 5, 6, 7], Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05")]], ["date", "store", "employee_ID", "end_date"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_v == inn_r1_t
    @test inn_r1_a == inn_r1_t
    @test inn_r1_v_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true)
    inn_r1_v =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, method = :hash)
    @test inn_r1_v == innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, accelerate = true, method = :hash)
    @test inn_r1_v_a ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true, accelerate = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "A", "A", "A", "B", "B", "B", "A", "A", "B", "B", "B", "B", "B", "B", "A", "A", "A", "B", "B", "B"], Union{Missing, Int64}[3, 4, 1, 2, 3, 4, 5, 6, 5, 1, 2, 7, 8, 5, 6, 7, 8, 1, 2, 3, 5, 6, 7]], ["date", "store", "employee_ID"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_v == inn_r1_t
    @test inn_r1_a == inn_r1_t
    @test inn_r1_v_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true)
    inn_r1_v =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, method = :hash)
    @test inn_r1_v ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true, method = :hash)
    @test inn_r1_v_a ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true, method = :hash)


    inn_r1_t = Dataset(date=Date[], store=String[], employee_ID=Int[])
    @test inn_r1 == inn_r1_t
    @test inn_r1_v == inn_r1_t
    @test inn_r1_a == inn_r1_t
    @test inn_r1_v_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, accelerate = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "B", "B", "B", "B", "B", "B", "A", "A", "A", "B", "A", "A", "B", "B"], Union{Missing, Int64}[4, 6, 7, 8, 6, 7, 8, 2, 3, 4, 8, 3, 4, 7, 8], Union{Missing, Date}[Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-06"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-05"), Date("2019-10-06")]], ["date", "store", "employee_ID", "end_date"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_a == inn_r1_t

    inn_r2 = innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = true)
    inn_r2_a = innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = true, accelerate = true)

    @test inn_r2 == innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = true, method = :hash)
    @test inn_r2_a == innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = true, accelerate = true, method = :hash)

    inn_r2_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "B", "B", "A", "B", "B", "A", "A", "B", "B"], Union{Missing, Int64}[4, 3, 5, 5, 1, 8, 7, 1, 2, 5, 6]], ["date", "store", "employee_ID"])
    @test inn_r2 == inn_r2_t
    @test inn_r2_a == inn_r2_t

    inn_r2 = innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = true, droprangecols = false)
    @test inn_r2 == innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = true, droprangecols = false, method = :hash)

    inn_r2_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "B", "B", "A", "B", "B", "A", "A", "B", "B"], Union{Missing, Int64}[4, 3, 5, 5, 1, 8, 7, 1, 2, 5, 6], Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-03"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-09-30"), Date("2019-10-02"), Date("2019-09-30"), Date("2019-10-02")], Union{Missing, Date}[Date("2019-10-06"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-06"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04")]], ["date", "store", "employee_ID", "start_date", "end_date"])
    @test inn_r2 == inn_r2_t
    inn_r2 = innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = [true, false], droprangecols = true)
    @test inn_r2 == innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], makeunique = true, stable = true, strict_inequality = [true, false], droprangecols = true, method = :hash)

    inn_r2_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "A", "A", "B", "B", "A", "B", "B", "B", "B", "B", "A", "A", "B", "B"], Union{Missing, Int64}[3, 4, 1, 2, 3, 5, 5, 1, 7, 8, 5, 6, 7, 1, 2, 5, 6]],["date", "store", "employee_ID"])
    @test inn_r2 == inn_r2_t
    push!(roster, ["C", 9, Date(2020), Date(2020)])
    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true)
    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "B", "B", "B", "B", "B", "B", "A", "A", "A", "B", "A", "A", "B", "B"], Union{Missing, Int64}[4, 6, 7, 8, 6, 7, 8, 2, 3, 4, 8, 3, 4, 7, 8], Union{Missing, Date}[Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-06"), Date("2019-10-05"), Date("2019-10-06"), Date("2019-10-05"), Date("2019-10-06")]], ["date", "store", "employee_ID", "end_date"])
    @test inn_r1 == inn_r1_t

    roster[4,3] = missing
    roster[6,4] = missing
    roster[8,3:4] .= missing
    roster[9,3:4] .= missing

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, accelerate = true)
    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, accelerate = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "B", "B", "B", "A", "B", "B", "B", "A", "A", "A", "B", "B", "B", "A", "A", "A", "B", "B"], Union{Missing, Int64}[2, 4, 2, 6, 8, 7, 2, 6, 8, 7, 3, 4, 2, 7, 8, 7, 3, 4, 2, 8, 7], Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-06"), missing, Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-06"), missing, missing, Date("2019-10-06"), Date("2019-10-04"), missing, Date("2019-10-06"), missing, missing, Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-06"), missing]], ["date", "store", "employee_ID", "end_date"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2020-01-01")], Union{Missing, String}["A", "A", "A"], Union{Missing, Int64}[2, 2, 2]], ["date", "store", "employee_ID"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true)
    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "A", "A", "B", "B", "A", "B", "A", "B", "B", "B", "B", "A", "A", "B", "B"], Union{Missing, Int64}[3, 4, 1, 3, 4, 5, 6, 3, 5, 1, 8, 5, 6, 8, 1, 3, 5, 6]], ["date", "store", "employee_ID"])
    @test inn_r1 == inn_r1_t
    MONTH(x) = month(x)
    MONTH(::Missing) = missing

    setformat!(store, 1=>MONTH)
    setformat!(roster, r"date"=>MONTH)
    inn_r3 = innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], droprangecols = false, strict_inequality = [true, false])
    @test inn_r3 == innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], droprangecols = false, strict_inequality = [true, false], method = :hash)

    inn_r3_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "B", "B", "A", "B", "B", "A", "B"], Union{Missing, Int64}[1, 1, 5, 5, 1, 5, 5, 1, 5], Union{Missing, Date}[Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30"), Date("2019-09-30")], Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04")]], ["date", "store", "employee_ID", "start_date", "end_date"])
    @test inn_r3 == inn_r3_t
    inn_r3 = innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], droprangecols = false, strict_inequality = [false, true])
    @test inn_r3 == innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], droprangecols = false, strict_inequality = [false, true], method = :hash)

    inn_r3_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "A"], Union{Missing, Int64}[3, 3, 3, 3], Union{Missing, Date}[Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, Date}[missing, missing, missing, missing]], ["date", "store", "employee_ID", "start_date", "end_date"])
    @test inn_r3 == inn_r3_t
    inn_r3 = innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], droprangecols = false, strict_inequality = [true, true])
    @test inn_r3 == innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], droprangecols = false, strict_inequality = [true, true], method = :hash)

    inn_r3_t = Dataset([Union{Missing, Date}[], Union{Missing, String}[], Union{Missing, Int64}[], Union{Missing, Date}[], Union{Missing, Date}[]], ["date", "store", "employee_ID", "start_date", "end_date"])
    @test inn_r3 == inn_r3_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false)
    inn_r1_v =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false, method = :hash)
    @test inn_r1_v ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false, accelerate = true, method = :hash)
    @test inn_r1_v_a ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (nothing, :start_date)], stable = true, mapformats = false, accelerate = true, method = :hash)


    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "B", "B", "B", "A", "B", "B", "B", "A", "A", "A", "B", "B", "B", "A", "A", "A", "B", "B"], Union{Missing, Int64}[2, 4, 2, 6, 8, 7, 2, 6, 8, 7, 3, 4, 2, 7, 8, 7, 3, 4, 2, 8, 7], Union{Missing, Date}[Date("2019-10-04"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-06"), missing, Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-06"), missing, missing, Date("2019-10-06"), Date("2019-10-04"), missing, Date("2019-10-06"), missing, missing, Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-06"), missing]], ["date", "store", "employee_ID", "end_date"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_v == inn_r1_t
    @test inn_r1_a == inn_r1_t
    @test inn_r1_v_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false)
    inn_r1_v =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false, method = :hash)
    @test inn_r1_v ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false, accelerate = true, method = :hash)
    @test inn_r1_v_a ==  innerjoin(store, view(roster, :, [1,2,4,3]), on = [:store => :store, :date => (:end_date, :start_date)], stable = true, mapformats = false, accelerate = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-04"), Date("2020-01-01")], Union{Missing, String}["A", "A", "A"], Union{Missing, Int64}[2, 2, 2]], ["date", "store", "employee_ID"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_v == inn_r1_t
    @test inn_r1_a == inn_r1_t
    @test inn_r1_v_a == inn_r1_t

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false)
    inn_r1_v =  innerjoin(store, view(roster, :, [1,2, 4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, [1,2, 4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false, accelerate = true)

    @test inn_r1 ==  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false, method = :hash)
    @test inn_r1_v ==  innerjoin(store, view(roster, :, [1,2, 4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false, method = :hash)
    @test inn_r1_a ==  innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false, accelerate = true, method = :hash)
    @test inn_r1_v_a == innerjoin(store, view(roster, :, [1,2, 4,3]), on = [:store => :store, :date => (:start_date, :end_date)], stable = true, mapformats = false, accelerate = true, method = :hash)

    inn_r1_t = Dataset([Union{Missing, Date}[Date("2019-10-05"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-10-02"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03"), Date("2019-10-03")], Union{Missing, String}["A", "A", "A", "A", "A", "B", "B", "A", "B", "A", "B", "B", "B", "B", "A", "A", "B", "B"], Union{Missing, Int64}[3, 4, 1, 3, 4, 5, 6, 3, 5, 1, 8, 5, 6, 8, 1, 3, 5, 6]], ["date", "store", "employee_ID"])
    @test inn_r1 == inn_r1_t
    @test inn_r1_v == inn_r1_t
    @test inn_r1_a == inn_r1_t
    @test inn_r1_v_a == inn_r1_t


    dsl = Dataset(x = [1,2,1,2], y = PooledArray([1.0, 5.0, 2.0, 1.0]))
    dsr = Dataset(x = [2,1,2], y1 = PooledArray([0, -1,1]), y2 = PooledArray([5,2,2]), z=[111,222,333])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], method = :hash, droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl = Dataset(x = [1,2,1,2], y = ([1.0, 5.0, 2.0, 1.0]))
    dsr = Dataset(x = [2,1,2], y1 = PooledArray([0, -1,1]), y2 = PooledArray([5,2,2]), z=[111,222,333])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], method = :hash, droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl = Dataset(x = [1,2,1,2], y = PooledArray([1.0, 5.0, 2.0, 1.0]))
    dsr = Dataset(x = [2,1,2], y1 = ([0, -1,1]), y2 = PooledArray([5,2,2]), z=[111,222,333])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], method = :hash, droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl = Dataset(x = [1,2,1,2], y = PooledArray([1.0, 5.0, 2.0, 1.0]))
    dsr = Dataset(x = [2,1,2], y1 = ([0, -1,1]), y2 = ([5,2,2]), z=[111,222,333])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], method = :hash, droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl = Dataset(x = [1,2,1,2], y = ([1.0, 5.0, 2.0, 1.0]))
    dsr = Dataset(x = [2,1,2], y1 = ([0, -1,1]), y2 = ([5,2,2]), z=[111,222,333])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], method = :hash, droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], method = :hash, droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    #views

    dsl1 = Dataset(x = [1,2,1,2], y = PooledArray([1.0, 5.0, 2.0, 1.0]))
    dsr1 = Dataset(x = [2,1,2], y1 = PooledArray([0, -1,1]), y2 = PooledArray([5,2,2]), z=[111,222,333])
    dsl = view(dsl1, [1,2,3,4], [1,2])
    dsr = view(dsr1, 1:3, 1:4)
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false, method = :hash) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl1 = Dataset(x = [1,2,1,2], y = ([1.0, 5.0, 2.0, 1.0]))
    dsr1 = Dataset(x = [2,1,2], y1 = PooledArray([0, -1,1]), y2 = PooledArray([5,2,2]), z=[111,222,333])
    dsl = view(dsl1, [1,2,3,4], [1,2])
    dsr = view(dsr1, 1:3, 1:4)
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false, method = :hash) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl1 = Dataset(x = [1,2,1,2], y = PooledArray([1.0, 5.0, 2.0, 1.0]))
    dsr1 = Dataset(x = [2,1,2], y1 = ([0, -1,1]), y2 = PooledArray([5,2,2]), z=[111,222,333])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false, method = :hash) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl1 = Dataset(x = [1,2,1,2], y = PooledArray([1.0, 5.0, 2.0, 1.0]))
    dsr1 = Dataset(x = [2,1,2], y1 = ([0, -1,1]), y2 = ([5,2,2]), z=[111,222,333])
    dsl = view(dsl1, [1,2,3,4], [1,2])
    dsr = view(dsr1, 1:3, 1:4)
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false, method = :hash) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    dsl1 = Dataset(x = [1,2,1,2], y = ([1.0, 5.0, 2.0, 1.0]))
    dsr1 = Dataset(x = [2,1,2], y1 = ([0, -1,1]), y2 = ([5,2,2]), z=[111,222,333])
    dsl = view(dsl1, [1,2,3,4], [1,2])
    dsr = view(dsr1, 1:3, 1:4)
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])

    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(x = [1,2,1,2,2], y = [1.0, 5,2,1,1], y1 = [-1,0,-1,1,0], y2 = [2,5,2,2,5], z= [222,111,222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2,2], y = [1.0,1,1], y1 = [-1,1,0], y2 = [2,2,5], z= [222,333,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, :y2)], droprangecols = false, strict_inequality = true, method = :hash) == Dataset(x = [1,2], y = [1.0,1], y1 = [-1,0], y2 = [2,5], z= [222,111])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(:y1, nothing)], droprangecols = false, method = :hash) == Dataset(x = [1,2,2,1,2,2], y=[1.0, 5,5,2,1,1], y1 = [-1,0,1,-1,0,1], y2=[2,5,2,2,5,2], z = [222,111,333,222,111,333])


    dsl1 = Dataset(x = [1,2,1,2], y = PooledArray([1.0, 5.0, 2.0, 1.0]))
    dsr1 = Dataset(x = [2,1,2], y1 = PooledArray([0, -1,1]), y2 = PooledArray([5,2,2]), z=[111,222,333])
    dsl = view(dsl1, [4,4,4,1,1,2,2], [2,1])
    dsr = view(dsr1, [3,1,2,2], [4,1,3,2])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(y = [fill(1.0, 10); fill(5,2)], x = [fill(2,6);fill(1,4);fill(2,2)], z = [repeat([333,111], 3); fill(222,4); fill(111,2)], y2 = [2,5,2,5,2,5,2,2,2,2,5,5], y1 = [1,0,1,0,1,0, -1,-1,-1,-1, 0, 0])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(y = [fill(1.0, 10); fill(5,2)], x = [fill(2,6);fill(1,4);fill(2,2)], z = [repeat([333,111], 3); fill(222,4); fill(111,2)], y2 = [2,5,2,5,2,5,2,2,2,2,5,5], y1 = [1,0,1,0,1,0, -1,-1,-1,-1, 0, 0])
    dsr = Dataset(view(dsr1, [3,1,2,2], [4,1,3,2]))
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(y = [fill(1.0, 10); fill(5,2)], x = [fill(2,6);fill(1,4);fill(2,2)], z = [repeat([333,111], 3); fill(222,4); fill(111,2)], y2 = [2,5,2,5,2,5,2,2,2,2,5,5], y1 = [1,0,1,0,1,0, -1,-1,-1,-1, 0, 0])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(y = [fill(1.0, 10); fill(5,2)], x = [fill(2,6);fill(1,4);fill(2,2)], z = [repeat([333,111], 3); fill(222,4); fill(111,2)], y2 = [2,5,2,5,2,5,2,2,2,2,5,5], y1 = [1,0,1,0,1,0, -1,-1,-1,-1, 0, 0])
    dsl = Dataset(view(dsl1, [4,4,4,1,1,2,2], [2,1]))
    dsr = view(dsr1, [3,1,2,2], [4,1,3,2])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false) == Dataset(y = [fill(1.0, 10); fill(5,2)], x = [fill(2,6);fill(1,4);fill(2,2)], z = [repeat([333,111], 3); fill(222,4); fill(111,2)], y2 = [2,5,2,5,2,5,2,2,2,2,5,5], y1 = [1,0,1,0,1,0, -1,-1,-1,-1, 0, 0])
    @test innerjoin(dsl, dsr, on = [:x=>:x, :y=>(nothing, :y2)], droprangecols = false, method = :hash) == Dataset(y = [fill(1.0, 10); fill(5,2)], x = [fill(2,6);fill(1,4);fill(2,2)], z = [repeat([333,111], 3); fill(222,4); fill(111,2)], y2 = [2,5,2,5,2,5,2,2,2,2,5,5], y1 = [1,0,1,0,1,0, -1,-1,-1,-1, 0, 0])


    dsl = Dataset(rand(1:10, 10, 3), [:x1,:x2, :x3])
    dsr = Dataset(rand(1:10, 4,3), [:x1, :x2, :y])
    l_ridx = [1,2,1,1,1,5,4,3,2,10]
    l_cidx = [3,1,2]
    r_ridx = [2,1,1,1,1,3]
    r_cidx = [1,3,2]

    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    dsl = Dataset(rand(1:10, 10, 3), [:x1,:x2, :x3])
    dsr = Dataset(rand(1:10, 4,3), [:x1, :x2, :y])
    for i in 1:3
        dsl[!, i]=PooledArray(dsl[!, i])
        dsr[!, i] = PooledArray(dsr[!, i])
    end

    l_ridx = [1,2,1,1,1,5,4,3,2,10]
    l_cidx = [3,1,2]
    r_ridx = [2,1,1,1,1,3]
    r_cidx = [1,3,2]

    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), dsr, on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, nothing)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(nothing, :y)], droprangecols = false, makeunique = true)
    @test innerjoin(view(dsl, l_ridx, l_cidx), view(dsr, r_ridx, r_cidx), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true, method = :hash) == innerjoin(Dataset(view(dsl, l_ridx, l_cidx)), Dataset(view(dsr, r_ridx, r_cidx)), on = [:x1=>:x1, :x2=>(:x2, :y)], droprangecols = false, makeunique = true, strict_inequality = true)

    dsl = Dataset(x1 = [1,2,1,3], y = [-1.2,-3,2.1,-3.5])
    dsr = Dataset(x1 = [1,2,3], lower = [0, -3,1], upper = [1,0,2])
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,3)]) == [0,1,0,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,3)], method = :hash) == [0,1,0,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,3)], strict_inequality = true) == [0,0,0,0]
    dsl = Dataset(x1 = [1,2,1,3], y = [-1.2,-3,2.1,-3.5])
    dsr = Dataset(x1 = [1,2,3], lower = [0, -3,1], upper = [3,0,2])
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,3)]) == [0,1,1,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,3)], method = :hash) == [0,1,1,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,3)], strict_inequality = true) == [0,0,1,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(nothing,3)]) == [1,1,1,1]
    @test contains(dsl, dsr, on = [1=>1, 2=>(nothing,3)], method = :hash) == [1,1,1,1]
    @test contains(dsl, dsr, on = [1=>1, 2=>(nothing,3)], method = :hash, strict_inequality = true) == [1,1,1,1]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,nothing)], method = :hash, strict_inequality = false) == [0,1,1,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,nothing)], method = :hash, strict_inequality = true) == [0,0,1,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(nothing,3)], method = :sort, strict_inequality = true) == [1,1,1,1]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,nothing)], method = :sort, strict_inequality = false) == [0,1,1,0]
    @test contains(dsl, dsr, on = [1=>1, 2=>(2,nothing)], method = :sort, strict_inequality = true) == [0,0,1,0]
end

@testset "update!, update" begin
    main = Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
    transaction = Dataset(group = ["G1", "G2"], id = [2, 1],
              x1 = [2.5, missing], x2 = [missing, 3])
    up1 = update(main, transaction, on = [:group, :id],
                 allowmissing = false, mode = :missing)
    up1_v = update(main, view(transaction, :, :), on = [:group, :id],
    allowmissing = false, mode = :missing)
    up1_a = update(main, transaction, on = [:group, :id],
    allowmissing = false, mode = :missing, accelerate = true)

    @test up1 == update(main, transaction, on = [:group, :id],
                 allowmissing = false, mode = :missing, method = :hash)
    @test up1_v == update(main, view(transaction, :, :), on = [:group, :id],
    allowmissing = false, mode = :missing, method = :hash)
    @test up1_a == update(main, transaction, on = [:group, :id],
    allowmissing = false, mode = :missing, accelerate = true, method = :hash)

    @test update(main, transaction, on = [:group, :id], op = +) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  4.8, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 4  ,missing, 2  ])
    @test update(main, transaction, on = [:group, :id], op = +, method = :hash) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  4.8, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 4  ,missing, 2  ])
    @test update(main, transaction, on = [:group, :id], op = +, allowmissing = true) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  4.8, missing, missing  , 0.0 ],
              x2    = [ 5  ,  4  ,  missing  ,  missing  , 4  ,missing, 2  ])
    @test update(main, transaction, on = [:group, :id], op = +, allowmissing = true, method = :hash) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  4.8, missing, missing  , 0.0 ],
              x2    = [ 5  ,  4  ,  missing  ,  missing  , 4  ,missing, 2  ])

    up1_t = Dataset([Union{Missing, String}["G1", "G1", "G1", "G1", "G2", "G2", "G2"], Union{Missing, Int64}[1, 1, 2, 2, 1, 1, 2], Union{Missing, Float64}[1.2, 2.3, 2.5, 2.3, 1.3, 2.1, 0.0], Union{Missing, Int64}[5, 4, 4, 2, 1, 3, 2]], ["group", "id", "x1", "x2"])
    @test up1 == up1_t
    @test up1_v == up1_t
    @test up1_a == up1_t

    up1 = update(main, transaction, on = [:group, :id],
    allowmissing = false, mode = :all)
    up1_v = update(main, view(transaction, :, :), on = [:group, :id],
    allowmissing = false, mode = :all)
    up1_a = update(main, transaction, on = [:group, :id],
    allowmissing = false, mode = :all, accelerate = true)

    @test up1 == update(main, transaction, on = [:group, :id],
    allowmissing = false, mode = :all, method = :hash)
    @test up1_v == update(main, view(transaction, :, :), on = [:group, :id],
    allowmissing = false, mode = :all, method = :hash)
    @test up1_a == update(main, transaction, on = [:group, :id],
    allowmissing = false, mode = :all, accelerate = true, method = :hash)


    up1_t = Dataset([Union{Missing, String}["G1", "G1", "G1", "G1", "G2", "G2", "G2"], Union{Missing, Int64}[1, 1, 2, 2, 1, 1, 2], Union{Missing, Float64}[1.2, 2.3, 2.5, 2.5, 1.3, 2.1, 0.0], Union{Missing, Int64}[5, 4, 4, 2, 3, 3, 2]],["group", "id", "x1", "x2"])
    @test up1 == up1_t
    @test up1_v == up1_t
    @test up1_a == up1_t

    up1 = update(main, transaction, on = [:group, :id],
                 allowmissing = true, mode = :all)
    @test up1 == update(main, transaction, on = [:group, :id],
              allowmissing = true, mode = :all, method = :hash)
    up1_t = Dataset([Union{Missing, String}["G1", "G1", "G1", "G1", "G2", "G2", "G2"], Union{Missing, Int64}[1, 1, 2, 2, 1, 1, 2], Union{Missing, Float64}[1.2, 2.3, 2.5, 2.5, missing, missing, 0.0], Union{Missing, Int64}[5, 4, missing, missing, 3, 3, 2]], ["group", "id", "x1", "x2"])
    @test up1 == up1_t

    up1 = update(main, transaction, on = [:group, :id],
    allowmissing = true, mode = :missing)
    up1_v = update(main, view(transaction, :, :), on = [:group, :id],
    allowmissing = true, mode = :missing)
    up1_a = update(main, transaction, on = [:group, :id],
    allowmissing = true, mode = :missing, accelerate = true)

    @test up1 == update(main, transaction, on = [:group, :id],
    allowmissing = true, mode = :missing, method = :hash)
    @test up1_v == update(main, view(transaction, :, :), on = [:group, :id],
    allowmissing = true, mode = :missing, method = :hash)
    @test up1_a == update(main, transaction, on = [:group, :id],
    allowmissing = true, mode = :missing, accelerate = true, method = :hash)


    up1_t = Dataset([Union{Missing, String}["G1", "G1", "G1", "G1", "G2", "G2", "G2"], Union{Missing, Int64}[1, 1, 2, 2, 1, 1, 2], Union{Missing, Float64}[1.2, 2.3, 2.5, 2.3, 1.3, 2.1, 0.0], Union{Missing, Int64}[5, 4, 4, 2, 1, 3, 2]], ["group", "id", "x1", "x2"])
    @test up1 == up1_t
    @test up1_v == up1_t
    @test up1_a == up1_t


    main = Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
    transaction = Dataset(group = ["G1", "G2"], id = [2, 1],
              x1 = [2.5, missing], x2 = [missing, 3])
    @test update(main, transaction, on = :group, mode = :all) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
              x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 2,2,2,2,1,1,1],
              x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
              x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)



    @test update(main, transaction, on = :group, method = :hash, mode = :all) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
              x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 2,2,2,2,1,1,1],
              x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
              x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group, method = :hash) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)

    update!(main, transaction, on = :group, mode = :missing)
    @test main == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    update!(main, transaction, on = :group, allowmissing = true, mode = :all)
    @test main == Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
              id    = [ 2,2,2,2,1,1,1],
              x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
              x2    = [ missing, missing, missing, missing, 3,3,3])

    main = Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
    transaction = Dataset(group = [3,1], id = [2, 1],
            x1 = [2.5, missing], x2 = [missing, 3])
    @test update(main, transaction, on = :group, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
            x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2,2,2,2,1,1,1],
            x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
            x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)

    @test update(main, transaction, on = :group, method = :hash, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
            x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2,2,2,2,1,1,1],
            x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
            x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group, method = :hash) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)

    update!(main, transaction, on = :group, mode = :missing)
    @test main == Dataset(group = [3,3,3,3,1,1,1],
          id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
          x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
          x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    update!(main, transaction, on = :group, allowmissing = true, mode = :all)
    @test main == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2,2,2,2,1,1,1],
            x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
            x2    = [ missing, missing, missing, missing, 3,3,3])

    main = Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
    transaction = Dataset(group = PooledArray([3,1]), id = [2, 1],
              x1 = [2.5, missing], x2 = [missing, 3])
    @test update(main, transaction, on = :group, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
              x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing) == Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 2,2,2,2,1,1,1],
              x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
              x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)

    @test update(main, transaction, on = :group, method = :hash, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
              x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 2,2,2,2,1,1,1],
              x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
              x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group, method = :hash) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)


    update!(main, transaction, on = :group, mode = :missing)
    @test main == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    update!(main, transaction, on = :group, allowmissing = true, mode = :all)
    @test main == Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 2,2,2,2,1,1,1],
              x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
              x2    = [ missing, missing, missing, missing, 3,3,3])

    main = Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
    transaction = Dataset(group = [3.0,1.0], id = [2, 1],
            x1 = [2.5, missing], x2 = [missing, 3])

    @test update(main, transaction, on = :group, op = max) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [2.5, 2.5,missing,  2.5, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 3  ,missing, 3  ])
    @test update(main, select(transaction, 4,1,2,3), on = :group, op = max) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [2.5, 2.5,missing,  2.5, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 3  ,missing, 3  ])
    @test update(main, transaction, on = :group, op = max, allowmissing = true) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [2.5, 2.5,missing,  2.5, missing, missing, missing ],
            x2    = [ missing, missing, missing, missing, 3  ,missing, 3  ])
    @test update(main, transaction, on = :group, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
            x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2,2,2,2,1,1,1],
            x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
            x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)

    @test update(main, transaction, on = :group, method = :hash, mode = :all) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2  ,  2  ,  2  ,  2  ,  1  ,  1  ,  1  ],
            x1    = [2.5, 2.5,2.5,2.5, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  ,3,3,3  ])
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
            x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
            x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2,2,2,2,1,1,1],
            x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
            x2    = [ missing, missing, missing, missing, 3,3,3])
    @test update(main, transaction, on = :group, method = :hash) == update(main, view(transaction, [2,1], :), on = :group)
    @test update(main, transaction, on = :group, mode = :missing, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, mode = :missing)
    @test update(main, transaction, on = :group, allowmissing = true, mode = :all, method = :hash) == update(main, view(transaction, [2,1], :), on = :group, allowmissing = true, mode = :all)

    update!(main, transaction, on = :group, mode = :missing)
    @test main == Dataset(group = [3,3,3,3,1,1,1],
          id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
          x1    = [1.2, 2.3,2.5,  2.3, 1.3, 2.1  , 0.0 ],
          x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,3, 2  ])
    update!(main, transaction, on = :group, allowmissing = true, mode = :all)
    @test main == Dataset(group = [3,3,3,3,1,1,1],
            id    = [ 2,2,2,2,1,1,1],
            x1    = [2.5,2.5,2.5,2.5, missing, missing, missing],
            x2    = [ missing, missing, missing, missing, 3,3,3])
    main = Dataset(group = [3,3,3,3,1,1,1],
              id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
              x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
              x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
    transaction = Dataset(group = [3.0,1.0], id = [2, 1],
              x1 = [2.5, missing], x2 = [missing, 3])
    update!(main, transaction, on = :group, mode = isequal(2.3))
    t_ds = Dataset([Union{Missing, Int64}[3, 3, 3, 3, 1, 1, 1], Union{Missing, Int64}[1, 1, 2, 2, 1, 1, 2], Union{Missing, Float64}[1.2, 2.5, missing, 2.5, 1.3, 2.1, 0.0], Union{Missing, Int64}[5, 4, 4, 2, 1, missing, 2]], [:group, :id, :x1, :x2])
    @test main == t_ds

end

@testset "compare, obs_id, multiple_match" begin

    ds1 = Dataset(x = ["a1", "a2", "a1", "a4"], y = [1,2,1,2], z = [1.2,0,-10,100])
    ds2 = Dataset(x = ["a10","a2", "a5"], y = [1,2,3], z=[2,200,-100])

    cmp_out = compare(ds1, ds2, cols = :z)
    @test cmp_out == Dataset("z=>z" => [false, false, false, missing])
    cmp_out = compare(ds1, ds2, on = [:y])
    @test cmp_out == Dataset([Union{Missing, Int64}[1, 2, 1, 2, 3], Union{Missing, Int32}[1, 2, 3, 4, missing], Union{Missing, Int32}[1, 2, 1, 2, 3], Union{Missing, Bool}[false, true, false, false, missing], Union{Missing, Bool}[false, false, false, false, missing]], ["y", "obs_id_left", "obs_id_right", "x=>x", "z=>z"])
    cmp_out = compare(ds1, ds2)
    @test cmp_out == Dataset("x=>x" => [false, true, false, missing], "y=>y" => [true, true, false, missing], "z=>z" => [false, false, false, missing])

    l1 = leftjoin(ds2, ds1, on = "y", multiple_match = true, makeunique = true, multiple_match_name = :multiple)

    @test isequal(l1[:, :multiple], [true, true, true, true, false])

    i1 = innerjoin(ds2, ds1, on = :x, multiple_match = true, multiple_match_name = :multiple, makeunique = true)
    @test i1[:, :multiple] == [false]
    i2 = innerjoin(ds2, ds1, on = :y, multiple_match = true, multiple_match_name = :multiple, makeunique = true)
    @test i2[:, :multiple] == [true, true, true, true]

    i3 = innerjoin(ds2, ds1, on = [:x, :y], multiple_match = true, multiple_match_name = :multiple, makeunique = true)
    @test i3[:, :multiple] == [false]


    l1 = leftjoin(view(ds2, :, [3,2,1]), ds1, on = "y", multiple_match = true, makeunique = true, multiple_match_name = :multiple)

    @test isequal(l1[:, :multiple], [true, true, true, true, false])

    i1 = innerjoin(view(ds2, :, :), view(ds1, :, :), on = :x, multiple_match = true, multiple_match_name = :multiple, makeunique = true)
    @test i1[:, :multiple] == [false]

    i2 = innerjoin(ds2, view(ds1, :, [3,2,1]), on = :y, multiple_match = true, multiple_match_name = :multiple, makeunique = true)
    @test i2[:, :multiple] == [true, true, true, true]

    i3 = innerjoin(view(ds2, [3,2,1], [3,2,1]), ds1, on = [:x, :y], multiple_match = true, multiple_match_name = :multiple, makeunique = true)
    @test i3[:, :multiple] == [false]

    i4 = innerjoin(ds1, ds2, on = [:x], multiple_match = true, multiple_match_name = :multiple, makeunique = true)
    @test i4[:, :multiple] == [false]

    i5 = innerjoin(ds1, ds2, on = [:x], multiple_match = true, multiple_match_name = :multiple, makeunique = true, obs_id = true, obs_id_name = :obs_id)

    @test i5[:, :obs_id_left] == [2]
    @test i5[:, :obs_id_right] == [2]

    o1 = outerjoin(ds1, ds2, on = [:x, :y], makeunique = true, obs_id = true, obs_id_name = :obs_id)
    @test isequal(o1[:, :obs_id_left], [1,2,3,4,missing,missing])
    @test isequal(o1[:, :obs_id_right], [missing,2,missing,missing,1,3])

    old = Dataset(Insurance_Id=[1,2,3,5],Business_Id=[10,20,30,50],
                     Amount=[100,200,300,missing],
                     Account_Id=["x1","x10","x5","x5"])
    new = Dataset(Ins_Id=[1,3,2,4,3,2],
                     B_Id=[10,40,30,40,30,20],
                     AMT=[100,200,missing,-500,350,700],
                     Ac_Id=["x1","x1","x10","x10","x7","x5"])
    eq_fun(x::Number, y::Number) = abs(x - y) <= 50
    eq_fun(x::AbstractString, y::AbstractString) = isequal(x,y)
    eq_fun(x,y) = missing
    cmp_out = compare(old, new,
                  on = [1=>1,2=>2],
                  cols = [:Amount=>:AMT, :Account_Id=>:Ac_Id],
                  eq = eq_fun)
    cmp_out_t = Dataset([Union{Missing, Int64}[1, 2, 3, 5, 2, 3, 4], Union{Missing, Int64}[10, 20, 30, 50, 30, 40, 40], Union{Missing, Int32}[1, 2, 3, 4, missing, missing, missing], Union{Missing, Int32}[1, 6, 5, missing, 3, 2, 4], Union{Missing, Bool}[true, false, true, missing, missing, missing, missing], Union{Missing, Bool}[true, false, false, missing, missing, missing, missing]], ["Insurance_Id", "Business_Id", "obs_id_left", "obs_id_right", "Amount=>AMT", "Account_Id=>Ac_Id"])
    @test cmp_out == cmp_out_t

    dsl = Dataset(x = 1:10)
    dsr = Dataset(x = 10:-1:1)
    l1 = leftjoin(dsl, dsr, on = :x, obs_id = true, obs_id_name = :obs_id)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(x = 1:10, y=1)
    dsr = Dataset(x = 10:-1:1, y=1)
    l1 = leftjoin(dsl, dsr, on = [:y, :x], obs_id = true, obs_id_name = :obs_id)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(x = 1:10, y=1)
    dsr = Dataset(x = 10:-1:1, y=1)
    l1 = leftjoin(dsl, dsr, on = [:y, :x], obs_id = true, obs_id_name = :obs_id, method = :hash)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(x = 1:10000, y=1)
    dsr = Dataset(x = 10000:-1:1, y=1)
    l1 = leftjoin(dsl, dsr, on = [:y, :x], obs_id = true, obs_id_name = :obs_id, threads = true)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(x = 1:10)
    dsr = Dataset(x = 10:-1:1)
    l1 = innerjoin(dsl, dsr, on = :x, obs_id = true, obs_id_name = :obs_id)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(x = 1:10, y=1)
    dsr = Dataset(x = 10:-1:1, y=1)
    l1 = innerjoin(dsl, dsr, on = [:y, :x], obs_id = true, obs_id_name = :obs_id)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(x = 1:10, y=1)
    dsr = Dataset(x = 10:-1:1, y=1)
    l1 = innerjoin(dsl, dsr, on = [:y, :x], obs_id = true, obs_id_name = :obs_id, method = :hash)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(x = 1:10000, y=1)
    dsr = Dataset(x = 10000:-1:1, y=1)
    l1 = innerjoin(dsl, dsr, on = [:y, :x], obs_id = true, obs_id_name = :obs_id, threads = true)
    @test l1[:, :obs_id_left] == 1:nrow(dsl)
    @test l1[:, :obs_id_right] == dsr[:, :x]

    dsl = Dataset(rand(1:10, 1000, 3), [:x1,:x2,:x3])
    dsr = Dataset(rand(1:10, 100, 3), [:y1,:y2,:y3])
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)

    dsl = Dataset(rand(1:2, 1000, 3), [:x1,:x2,:x3])
    dsr = Dataset(rand(1:2, 100, 3), [:y1,:y2,:y3])
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)

    dsl = Dataset(rand(1:100, 1000, 3), [:x1,:x2,:x3])
    dsr = Dataset(rand(1:110, 100, 3), [:y1,:y2,:y3])
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)


    dsl = Dataset(rand(-10:1000, 1000, 3), [:x1,:x2,:x3])
    dsr = Dataset(rand(-1:1100, 100, 3), [:y1,:y2,:y3])
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)



    #########
    dsl = Dataset(rand(1:10, 1000, 3), [:x1,:x2,:x3])
    modify!(dsl, :x1=>byrow(x->x * 1.1))
    dsr = Dataset(rand(1:10, 100, 3), [:y1,:y2,:y3])
    modify!(dsr, :y1=>byrow(x->x * 1.1))
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)

    dsl = Dataset(rand(1:2, 1000, 3), [:x1,:x2,:x3])
    modify!(dsl, :x1=>byrow(x->x * 1.1))
    dsr = Dataset(rand(1:2, 100, 3), [:y1,:y2,:y3])
    modify!(dsr, :y1=>byrow(x->x * 1.1))
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)

    dsl = Dataset(rand(1:100, 1000, 3), [:x1,:x2,:x3])
    modify!(dsl, :x1=>byrow(x->x * 1.1))
    dsr = Dataset(rand(1:110, 100, 3), [:y1,:y2,:y3])
    modify!(dsr, :y1=>byrow(x->x * 1.1))
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)


    dsl = Dataset(rand(-10:1000, 1000, 3), [:x1,:x2,:x3])
    modify!(dsl, :x1=>byrow(x->x * 1.1))
    dsr = Dataset(rand(-1:1100, 100, 3), [:y1,:y2,:y3])
    modify!(dsr, :y1=>byrow(x->x * 1.1))
    unique!(dsr, 1:2)
    insertcols!(dsl, :obs_left => 1:nrow(dsl))
    insertcols!(dsr, :obs_right => 1:nrow(dsr))

    i1 = innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>(:y2,:y3)], multiple_match = true, obs_id = true, check = false)
    i2 = innerjoin(dsl, dsr, on = [1=>1,2=>2], multiple_match = true, obs_id = true)
    l1 = leftjoin(dsl, dsr, on = [:x1=>:y1], multiple_match = true, obs_id = true, check = false)
    l2 = leftjoin(dsl, dsr, on = [2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)
    l3 = leftjoin(dsl, dsr, on = [1=>1, 2=>2, 3=>3], multiple_match = true, obs_id = true, check = false)

    @test isequal(i1.obs_id_left, i1.obs_left)
    @test isequal(i1.obs_id_right, i1.obs_right)
    @test isequal(i2.obs_id_left, i2.obs_left)
    @test isequal(i2.obs_id_right, i2.obs_right)
    @test isequal(l1.obs_id_left, l1.obs_left)
    @test isequal(l1.obs_id_right, l1.obs_right)
    @test isequal(l2.obs_id_left, l2.obs_left)
    @test isequal(l2.obs_id_right, l2.obs_right)
    @test isequal(l3.obs_id_left, l3.obs_left)
    @test isequal(l3.obs_id_right, l3.obs_right)

    if !isempty(i1)
        modify!(groupby(i1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i1.multiple, i1.mm)
    end
    if !isempty(i2)
        modify!(groupby(i2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
        @test isequal(i2.multiple, i2.mm)
    end
    modify!(groupby(l1, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l2, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)
    modify!(groupby(l3, :obs_id_left), :obs_id_left => (x->length(x)>1)=>:mm)

    @test isequal(l1.multiple, l1.mm)
    @test isequal(l2.multiple, l2.mm)
    @test isequal(l3.multiple, l3.mm)






end
