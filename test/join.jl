
using Test, InMemoryDatasets, Random, CategoricalArrays, PooledArrays
const ≅ = isequal

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

@testset "general usage" begin
    # Join on symbols or vectors of symbols
    innerjoin(name, job, on = :ID)
    innerjoin(name, job, on = [:ID])

    @test_throws ArgumentError innerjoin(name, job)
    @test_throws MethodError innerjoin(name, job, on = :ID, matchmissing=:errors)
    @test_throws MethodError outerjoin(name, job, on = :ID, matchmissing=:notequal)

    @test innerjoin(name, job, on = :ID) == inner
    @test outerjoin(name, job, on = :ID) == outer
    @test leftjoin(name, job, on = :ID) == left
    @test semijoin(name, job, on = :ID) == semi
    @test antijoin(name, job, on = :ID) == anti
    @test closejoin(classA, grades, on = :mark) == closeone
    @test closejoin(trades, quotes, on = :time, makeunique = true) == closefinance1

    # Join with no non-key columns
    on = [:ID]
    nameid = name[:, on]
    jobid = job[:, on]

    @test innerjoin(nameid, jobid, on = :ID) == inner[:, on]
    @test outerjoin(nameid, jobid, on = :ID) == outer[:, on]
    @test leftjoin(nameid, jobid, on = :ID) == left[:, on]
    @test semijoin(nameid, jobid, on = :ID) == semi[:, on]
    @test antijoin(nameid, jobid, on = :ID) == anti[:, on]

    # Join on multiple keys
    ds1 = Dataset(A = 1, B = 2, C = 3)
    ds2 = Dataset(A = 1, B = 2, D = 4)

    @test innerjoin(ds1, ds2, on = [:A, :B]) == Dataset(A = 1, B = 2, C = 3, D = 4)

    dsl = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
         Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
         Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
         Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], ["x1", "x2", "x3", "row"])
    dsr = Dataset(x1=[1, 3], y =[100.0, 200.0])
    setformat!(dsl, 1=>iseven)
    setformat!(dsr, 1=>isodd)

    left1 = leftjoin(dsl, dsr, on = :x1)
    left1_t = Dataset([Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[3, 3, 6, 7, 7, 10, 10, 5, 10, 10, 9, 1, 1, 1, 1],
           Union{Missing, Int64}[1, 1, 2, 3, 3, 4, 5, 6, 7, 7, 8, 9, 9, 10, 10],
           Union{Missing, Float64}[100.0, 200.0, missing, 100.0, 200.0, missing, missing, missing, 100.0, 200.0, missing, 100.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    left2 = leftjoin(dsl, dsr, on = :x1, mapformats = [true, false])
    left2_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, missing, 100.0, missing, missing, missing, 100.0, missing, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    left3 = leftjoin(dsl, dsr, on = :x1, mapformats = [false, true])
    left3_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, missing, missing, 100.0, 200.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    left4 = leftjoin(dsl, dsr, on = :x1, mapformats = [false, false])
    left4_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, 200.0, missing, 100.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    inner1 = innerjoin(dsl, dsr, on = :x1)
    inner1_t = Dataset([Union{Missing, Int64}[10, 10, 4, 4, 6, 6, 2, 2, 10, 10],
           Union{Missing, Int64}[10, 10, 4, 4, 6, 6, 2, 2, 10, 10],
           Union{Missing, Int64}[3, 3, 7, 7, 10, 10, 1, 1, 1, 1],
           Union{Missing, Int64}[1, 1, 3, 3, 7, 7, 9, 9, 10, 10],
           Union{Missing, Float64}[100.0, 200.0, 100.0, 200.0, 100.0, 200.0, 100.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    inner2 = innerjoin(dsl, dsr, on = :x1, mapformats = [true, false])
    inner2_t = Dataset([ Union{Missing, Int64}[10, 4, 6, 2, 10],
           Union{Missing, Int64}[10, 4, 6, 2, 10],
           Union{Missing, Int64}[3, 7, 10, 1, 1],
           Union{Missing, Int64}[1, 3, 7, 9, 10],
           Union{Missing, Float64}[100.0, 100.0, 100.0, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    inner3 = innerjoin(dsl, dsr, on = :x1, mapformats = [false, true])
    inner3_t = Dataset([Union{Missing, Int64}[1, 1],
           Union{Missing, Int64}[1, 1],
           Union{Missing, Int64}[10, 10],
           Union{Missing, Int64}[4, 4],
           Union{Missing, Float64}[100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    inner4 = innerjoin(dsl, dsr, on = :x1, mapformats = [false, false])
    inner4_t = Dataset([Union{Missing, Int64}[3, 1],
           Union{Missing, Int64}[3, 1],
           Union{Missing, Int64}[6, 10],
           Union{Missing, Int64}[2, 4],
           Union{Missing, Float64}[200.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    outer1 = outerjoin(dsl, dsr, on = :x1)
    outer1_t = Dataset([Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[10, 10, 3, 4, 4, 1, 5, 5, 6, 6, 7, 2, 2, 10, 10],
           Union{Missing, Int64}[3, 3, 6, 7, 7, 10, 10, 5, 10, 10, 9, 1, 1, 1, 1],
           Union{Missing, Int64}[1, 1, 2, 3, 3, 4, 5, 6, 7, 7, 8, 9, 9, 10, 10],
           Union{Missing, Float64}[100.0, 200.0, missing, 100.0, 200.0, missing, missing, missing, 100.0, 200.0, missing, 100.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    outer2 = outerjoin(dsl, dsr, on = :x1, mapformats = [false, true])
    outer2_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, missing, missing, 100.0, 200.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    outer3 = outerjoin(dsl, dsr, on = :x1, mapformats = [true, false])
    outer3_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10, 3],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10, missing],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1, missing],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, missing],
           Union{Missing, Float64}[100.0, missing, 100.0, missing, missing, missing, 100.0, missing, 100.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    outer4 = outerjoin(dsl, dsr, on = :x1, mapformats = [false, false])
    outer4_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, 200.0, missing, 100.0, missing, missing, missing, missing, missing, missing]], ["x1", "x2", "x3", "row", "y"])
    contains1 = contains(dsl, dsr, on = :x1)
    contains1_t = Bool[1, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    contains2 = contains(dsl, dsr, on = :x1, mapformats = [true, false])
    contains2_t = Bool[1, 0, 1, 0, 0, 0, 1, 0, 1, 1]
    contains3 = contains(dsl, dsr, on = :x1, mapformats =[false, true])
    contains3_t = Bool[0, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    contains4 = contains(dsl, dsr, on = :x1, mapformats = [false, false])
    contains4_t = Bool[0, 1, 0, 1, 0, 0, 0, 0, 0, 0]

    close1 = closejoin(dsl, dsr, on = :x1)
    close1_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, missing, 200.0, missing, missing, missing, 200.0, missing, 200.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close2 = closejoin(dsl, dsr, on = :x1, direction = :forward)
    close2_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    close3 = closejoin(dsl, dsr, on = :x1, border = :nearest)
    close3_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 100.0, 200.0, 100.0, 100.0, 100.0, 200.0, 100.0, 200.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close4 = closejoin(dsl, dsr, on = :x1, mapformats = [true, false])
    close4_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, missing, 100.0, missing, missing, missing, 100.0, missing, 100.0, 100.0]],  ["x1", "x2", "x3", "row", "y"])
    close5 = closejoin(dsl, dsr, on = :x1, mapformats = [true, false], direction = :forward)
    close5_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]], ["x1", "x2", "x3", "row", "y"])
    close6 = closejoin(dsl, dsr, on = :x1, mapformats = [false, true])
    close6_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close7 = closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward)
    close7_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, missing, missing, 100.0, missing, missing, missing, missing, missing, missing]],["x1", "x2", "x3", "row", "y"])
    close8 = closejoin(dsl, dsr, on = :x1, mapformats = [false, true], direction = :forward, border = :nearest)
    close8_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 200.0, 200.0, 100.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0]],["x1", "x2", "x3", "row", "y"])
    close9 = closejoin(dsl, dsr, on = :x1, mapformats = [false, false])
    close9_t = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[200.0, 200.0, 200.0, 100.0, 200.0, 200.0, 200.0, 200.0, 100.0, 200.0]], ["x1", "x2", "x3", "row", "y"])
    close10 = closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward)
    close10_t = Dataset([ Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
           Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
           Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
           Union{Missing, Float64}[missing, 200.0, missing, 100.0, missing, missing, missing, missing, 200.0, missing]], ["x1", "x2", "x3", "row", "y"])
    close11 = closejoin(dsl, dsr, on = :x1, mapformats = false, direction = :forward, border = :nearest)
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

end

@testset "all joins" begin
    ds1 = Dataset(A = categorical(1:50),
                    B = categorical(1:50),
                    C = 1)
    @test innerjoin(ds1, ds1, on = [:A, :B], makeunique=true)[!, 1:3] == ds1
    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1088)
    ds = Dataset(Name = Union{String, Missing}["A", "B", "C"],
                Mass = [1.5, 2.2, 1.1])
    ds2 = Dataset(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test leftjoin(ds2, ds, on=:Name) == Dataset(Name = ["A", "B", "C", "A"],
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
    dsl = Dataset(x=[1,2], y=[3,4])
    re = innerjoin(dsl, dsl, on = [:x=>:y], makeunique = true)
    @test Dataset([[],[],[]], names(re)) == re


    dsl = Dataset(x1 = [1,2,3,4,5,6], x2= [1,1,1,2,2,2])
    dsr = Dataset(x1 = [1,1,1,4,5,7],x2= [1,1,3,4,5,6], y = [343,54,54,464,565,7567])
    cj = closejoin(dsl, dsr, on = [:x1, :x2])
    cj_t = Dataset([Union{Missing, Int64}[1, 2, 3, 4, 5, 6],
         Union{Missing, Int64}[1, 1, 1, 2, 2, 2],
         Union{Missing, Int64}[54, missing, missing, missing, missing, missing]], ["x1", "x2", "y"])
    @test cj == cj_t
    cj = closejoin(dsl, dsr, on = [:x1, :x2], direction = :forward)
    cj_t = Dataset([ Union{Missing, Int64}[1, 2, 3, 4, 5, 6],
         Union{Missing, Int64}[1, 1, 1, 2, 2, 2],
         Union{Missing, Int64}[343, missing, missing, 464, 565, missing]],["x1", "x2", "y"] )
    @test cj == cj_t
end
#
# @testset "all joins with CategoricalArrays" begin
#     ds1 = Dataset(Any[CategoricalArray([1, 3, 5]),
#                         CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
#     ds2 = Dataset(Any[CategoricalArray([0, 1, 2, 3, 4]),
#                         CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])
#
#     @test crossjoin(ds1, ds2, makeunique=true) ==
#         Dataset([repeat([1, 3, 5], inner = 5),
#                    repeat([1, 3, 5], inner = 5),
#                    repeat([0, 1, 2, 3, 4], outer = 3),
#                    repeat([0, 1, 2, 3, 4], outer = 3)],
#                   [:id, :fid, :id_1, :fid_1])
#     @test all(isa.(eachcol(crossjoin(ds1, ds2, makeunique=true)),
#                    [CategoricalVector{T} for T in (Int, Float64, Int, Float64)]))
#
#     i(on) = innerjoin(ds1, ds2, on = on, makeunique=true)
#     l(on) = leftjoin(ds1, ds2, on = on, makeunique=true)
#     r(on) = rightjoin(ds1, ds2, on = on, makeunique=true)
#     o(on) = outerjoin(ds1, ds2, on = on, makeunique=true)
#     s(on) = semijoin(ds1, ds2, on = on, makeunique=true)
#     a(on) = antijoin(ds1, ds2, on = on, makeunique=true)
#
#     @test s(:id) ==
#           s(:fid) ==
#           s([:id, :fid]) == Dataset([[1, 3], [1, 3]], [:id, :fid])
#     @test typeof.(eachcol(s(:id))) ==
#           typeof.(eachcol(s(:fid))) ==
#           typeof.(eachcol(s([:id, :fid])))
#     @test all(isa.(eachcol(s(:id)),
#                    [CategoricalVector{T} for T in (Int, Float64)]))
#
#     @test a(:id) ==
#           a(:fid) ==
#           a([:id, :fid]) == Dataset([[5], [5]], [:id, :fid])
#     @test typeof.(eachcol(a(:id))) ==
#           typeof.(eachcol(a(:fid))) ==
#           typeof.(eachcol(a([:id, :fid])))
#     @test all(isa.(eachcol(a(:id)),
#                    [CategoricalVector{T} for T in (Int, Float64)]))
#
#     on = :id
#     @test i(on) == Dataset([[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
#     @test all(isa.(eachcol(i(on)),
#                    [CategoricalVector{T} for T in (Int, Float64, Float64)]))
#     @test l(on) ≅ Dataset(id = [1, 3, 5],
#                             fid = [1, 3, 5],
#                             fid_1 = [1, 3, missing])
#     @test all(isa.(eachcol(l(on)),
#                    [CategoricalVector{T} for T in (Int, Float64, Union{Float64, Missing})]))
#     @test r(on) ≅ Dataset(id = [1, 3, 0, 2, 4],
#                             fid = [1, 3, missing, missing, missing],
#                             fid_1 = [1, 3, 0, 2, 4])
#     @test all(isa.(eachcol(r(on)),
#                    [CategoricalVector{T} for T in (Int, Union{Float64, Missing}, Float64)]))
#     @test o(on) ≅ Dataset(id = [1, 3, 5, 0, 2, 4],
#                             fid = [1, 3, 5, missing, missing, missing],
#                             fid_1 = [1, 3, missing, 0, 2, 4])
#     @test all(isa.(eachcol(o(on)),
#                    [CategoricalVector{T} for T in (Int, Union{Float64, Missing}, Union{Float64, Missing})]))
#
#     on = :fid
#     @test i(on) == Dataset([[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
#     @test all(isa.(eachcol(i(on)),
#                    [CategoricalVector{T} for T in (Int, Float64, Int)]))
#     @test l(on) ≅ Dataset(id = [1, 3, 5],
#                             fid = [1, 3, 5],
#                             id_1 = [1, 3, missing])
#     @test all(isa.(eachcol(l(on)),
#                    [CategoricalVector{T} for T in (Int, Float64, Union{Int, Missing})]))
#     @test r(on) ≅ Dataset(id = [1, 3, missing, missing, missing],
#                             fid = [1, 3, 0, 2, 4],
#                             id_1 = [1, 3, 0, 2, 4])
#     @test all(isa.(eachcol(r(on)),
#                    [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Int)]))
#     @test o(on) ≅ Dataset(id = [1, 3, 5, missing, missing, missing],
#                             fid = [1, 3, 5, 0, 2, 4],
#                             id_1 = [1, 3, missing, 0, 2, 4])
#     @test all(isa.(eachcol(o(on)),
#                    [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Union{Int, Missing})]))
#
#     on = [:id, :fid]
#     @test i(on) == Dataset([[1, 3], [1, 3]], [:id, :fid])
#     @test all(isa.(eachcol(i(on)),
#                    [CategoricalVector{T} for T in (Int, Float64)]))
#     @test l(on) == Dataset(id = [1, 3, 5],
#                              fid = [1, 3, 5])
#     @test all(isa.(eachcol(l(on)),
#                    [CategoricalVector{T} for T in (Int, Float64)]))
#     @test r(on) == Dataset(id = [1, 3, 0, 2, 4],
#                              fid = [1, 3, 0, 2, 4])
#     @test all(isa.(eachcol(r(on)),
#                    [CategoricalVector{T} for T in (Int, Float64)]))
#     @test o(on) == Dataset(id = [1, 3, 5, 0, 2, 4],
#                              fid = [1, 3, 5, 0, 2, 4])
#     @test all(isa.(eachcol(o(on)),
#                    [CategoricalVector{T} for T in (Int, Float64)]))
# end
#
# @testset "maintain CategoricalArray levels ordering on join - non-`on` cols" begin
#     A = Dataset(a = [1, 2, 3], b = ["a", "b", "c"])
#     B = Dataset(b = ["a", "b", "c"], c = CategoricalVector(["a", "b", "b"]))
#     levels!(B.c, ["b", "a"])
#     @test levels(innerjoin(A, B, on=:b).c) == ["b", "a"]
#     @test levels(innerjoin(B, A, on=:b).c) == ["b", "a"]
#     @test levels(leftjoin(A, B, on=:b).c) == ["b", "a"]
#     @test levels(rightjoin(A, B, on=:b).c) == ["b", "a"]
#     @test levels(outerjoin(A, B, on=:b).c) == ["b", "a"]
#     @test levels(semijoin(B, A, on=:b).c) == ["b", "a"]
# end
#
# @testset "maintain CategoricalArray levels ordering on join - ordering conflicts" begin
#     A = Dataset(a = [1, 2, 3, 4], b = CategoricalVector(["a", "b", "c", "d"]))
#     levels!(A.b, ["d", "c", "b", "a"])
#     B = Dataset(b = CategoricalVector(["a", "b", "c"]), c = [5, 6, 7])
#     @test levels(innerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(innerjoin(B, A, on=:b).b) == ["a", "b", "c"]
#     @test levels(leftjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(leftjoin(B, A, on=:b).b) == ["a", "b", "c"]
#     @test levels(rightjoin(A, B, on=:b).b) == ["a", "b", "c"]
#     @test levels(rightjoin(B, A, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(outerjoin(B, A, on=:b).b) == ["d", "a", "b", "c"]
#     @test levels(outerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(semijoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(semijoin(B, A, on=:b).b) == ["a", "b", "c"]
# end
#
# @testset "maintain CategoricalArray levels ordering on join - left is categorical" begin
#     A = Dataset(a = [1, 2, 3, 4], b = CategoricalVector(["a", "b", "c", "d"]))
#     levels!(A.b, ["d", "c", "b", "a"])
#     B = Dataset(b = ["a", "b", "c"], c = [5, 6, 7])
#     @test levels(innerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(innerjoin(B, A, on=:b).b) == ["a", "b", "c"]
#     @test levels(leftjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(leftjoin(B, A, on=:b).b) == ["a", "b", "c"]
#     @test levels(rightjoin(A, B, on=:b).b) == ["a", "b", "c"]
#     @test levels(rightjoin(B, A, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(outerjoin(A, B, on=:b).b) == ["a", "b", "c", "d"]
#     @test levels(outerjoin(B, A, on=:b).b) == ["a", "b", "c", "d"]
#     @test levels(semijoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
#     @test levels(semijoin(B, A, on=:b).b) == ["a", "b", "c"]
# end
#
# @testset "join on columns with different left/right names" begin
#     left = Dataset(id = 1:7, sid = string.(1:7))
#     right = Dataset(ID = 3:10, SID = string.(3:10))
#
#     @test innerjoin(left, right, on = :id => :ID) ==
#         Dataset(id = 3:7, sid = string.(3:7), SID = string.(3:7))
#     @test innerjoin(left, right, on = [:id => :ID]) ==
#         Dataset(id = 3:7, sid = string.(3:7), SID = string.(3:7))
#     @test innerjoin(left, right, on = [:id => :ID, :sid => :SID]) ==
#         Dataset(id = 3:7, sid = string.(3:7))
#
#     @test leftjoin(left, right, on = :id => :ID) ≅
#         Dataset(id = [3:7; 1:2], sid = string.([3:7; 1:2]),
#                   SID = [string.(3:7)..., missing, missing])
#     @test leftjoin(left, right, on = [:id => :ID]) ≅
#         Dataset(id = [3:7; 1:2], sid = string.([3:7; 1:2]),
#                   SID = [string.(3:7)..., missing, missing])
#     @test leftjoin(left, right, on = [:id => :ID, :sid => :SID]) ==
#         Dataset(id = [3:7; 1:2], sid = string.([3:7; 1:2]))
#
#     @test rightjoin(left, right, on = :id => :ID) ≅
#         Dataset(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
#                  SID = string.(3:10))
#     @test rightjoin(left, right, on = [:id => :ID]) ≅
#         Dataset(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
#                  SID = string.(3:10))
#     @test rightjoin(left, right, on = [:id => :ID, :sid => :SID]) ≅
#         Dataset(id = 3:10, sid = string.(3:10))
#
#     @test outerjoin(left, right, on = :id => :ID) ≅
#         Dataset(id = [3:7; 1:2; 8:10], sid = [string.([3:7; 1:2])..., missing, missing, missing],
#                   SID = [string.(3:7)..., missing, missing, string.(8:10)...])
#     @test outerjoin(left, right, on = [:id => :ID]) ≅
#         Dataset(id = [3:7; 1:2; 8:10], sid = [string.([3:7; 1:2])..., missing, missing, missing],
#                   SID = [string.(3:7)..., missing, missing, string.(8:10)...])
#     @test outerjoin(left, right, on = [:id => :ID, :sid => :SID]) ≅
#         Dataset(id = [3:7; 1:2; 8:10], sid = string.([3:7; 1:2; 8:10]))
#
#     @test semijoin(left, right, on = :id => :ID) ==
#         Dataset(id = 3:7, sid = string.(3:7))
#     @test semijoin(left, right, on = [:id => :ID]) ==
#         Dataset(id = 3:7, sid = string.(3:7))
#     @test semijoin(left, right, on = [:id => :ID, :sid => :SID]) ==
#         Dataset(id = 3:7, sid = string.(3:7))
#
#     @test antijoin(left, right, on = :id => :ID) ==
#         Dataset(id = 1:2, sid = string.(1:2))
#     @test antijoin(left, right, on = [:id => :ID]) ==
#         Dataset(id = 1:2, sid = string.(1:2))
#     @test antijoin(left, right, on = [:id => :ID, :sid => :SID]) ==
#         Dataset(id = 1:2, sid = string.(1:2))
#
#     @test_throws ArgumentError innerjoin(left, right, on = (:id, :ID))
# end
#
# @testset "join with a column of type Any" begin
#     l = Dataset(a=Any[1:7;], b=[1:7;])
#     r = Dataset(a=Any[3:10;], b=[3:10;])
#
#     # join by :a and :b (Any is the on-column)
#     @test innerjoin(l, r, on=[:a, :b]) ≅ Dataset(a=Any[3:7;], b=3:7)
#     @test eltype.(eachcol(innerjoin(l, r, on=[:a, :b]))) == [Any, Int]
#
#     @test leftjoin(l, r, on=[:a, :b]) ≅ Dataset(a=Any[3:7;1:2], b=[3:7; 1:2])
#     @test eltype.(eachcol(leftjoin(l, r, on=[:a, :b]))) == [Any, Int]
#
#     @test rightjoin(l, r, on=[:a, :b]) ≅ Dataset(a=Any[3:10;], b=3:10)
#     @test eltype.(eachcol(rightjoin(l, r, on=[:a, :b]))) == [Any, Int]
#
#     @test outerjoin(l, r, on=[:a, :b]) ≅ Dataset(a=Any[3:7; 1:2; 8:10], b=[3:7; 1:2; 8:10])
#     @test eltype.(eachcol(outerjoin(l, r, on=[:a, :b]))) == [Any, Int]
#
#     # join by :b (Any is not on-column)
#     @test innerjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=Any[3:7;], b=3:7, a_1=Any[3:7;])
#     @test eltype.(eachcol(innerjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]
#
#     @test leftjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=Any[3:7; 1:2], b=[3:7; 1:2], a_1=[3:7; missing; missing])
#     @test eltype.(eachcol(leftjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]
#
#     @test rightjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=[3:7; fill(missing, 3)], b=3:10, a_1=Any[3:10;])
#     @test eltype.(eachcol(rightjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]
#
#     @test outerjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=[3:7; 1:2; missing; missing; missing], b=[3:7; 1:2; 8:10],
#                   a_1=[3:7; missing; missing; 8:10])
#     @test eltype.(eachcol(outerjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]
# end
#
# @testset "joins with categorical columns and no matching rows" begin
#     l = Dataset(a=1:3, b=categorical(["a", "b", "c"]))
#     r = Dataset(a=4:5, b=categorical(["d", "e"]))
#     nl = size(l, 1)
#     nr = size(r, 1)
#
#     CS = eltype(l.b)
#
#     # joins by a and b
#     @test innerjoin(l, r, on=[:a, :b]) ≅ Dataset(a=Int[], b=similar(l.a, 0))
#     @test eltype.(eachcol(innerjoin(l, r, on=[:a, :b]))) == [Int, CS]
#
#     @test leftjoin(l, r, on=[:a, :b]) ≅ Dataset(a=l.a, b=l.b)
#     @test eltype.(eachcol(leftjoin(l, r, on=[:a, :b]))) == [Int, CS]
#
#     @test rightjoin(l, r, on=[:a, :b]) ≅ Dataset(a=r.a, b=r.b)
#     @test eltype.(eachcol(rightjoin(l, r, on=[:a, :b]))) == [Int, CS]
#
#     @test outerjoin(l, r, on=[:a, :b]) ≅
#         Dataset(a=vcat(l.a, r.a), b=vcat(l.b, r.b))
#     @test eltype.(eachcol(outerjoin(l, r, on=[:a, :b]))) == [Int, CS]
#
#     # joins by a
#     @test innerjoin(l, r, on=:a, makeunique=true) ≅
#         Dataset(a=Int[], b=similar(l.b, 0), b_1=similar(r.b, 0))
#     @test eltype.(eachcol(innerjoin(l, r, on=:a, makeunique=true))) == [Int, CS, CS]
#
#     @test leftjoin(l, r, on=:a, makeunique=true) ≅
#         Dataset(a=l.a, b=l.b, b_1=similar_missing(r.b, nl))
#     @test eltype.(eachcol(leftjoin(l, r, on=:a, makeunique=true))) ==
#         [Int, CS, Union{CS, Missing}]
#
#     @test rightjoin(l, r, on=:a, makeunique=true) ≅
#         Dataset(a=r.a, b=similar_missing(l.b, nr), b_1=r.b)
#     @test eltype.(eachcol(rightjoin(l, r, on=:a, makeunique=true))) ==
#         [Int, Union{CS, Missing}, CS]
#
#     @test outerjoin(l, r, on=:a, makeunique=true) ≅
#         Dataset(a=vcat(l.a, r.a),
#                   b=vcat(l.b, fill(missing, nr)),
#                   b_1=vcat(fill(missing, nl), r.b))
#     @test eltype.(eachcol(outerjoin(l, r, on=:a, makeunique=true))) ==
#         [Int, Union{CS, Missing}, Union{CS, Missing}]
#
#     # joins by b
#     @test innerjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=Int[], b=similar(l.b, 0), a_1=similar(r.b, 0))
#     @test eltype.(eachcol(innerjoin(l, r, on=:b, makeunique=true))) == [Int, CS, Int]
#
#     @test leftjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=l.a, b=l.b, a_1=fill(missing, nl))
#     @test eltype.(eachcol(leftjoin(l, r, on=:b, makeunique=true))) ==
#         [Int, CS, Union{Int, Missing}]
#
#     @test rightjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=fill(missing, nr), b=r.b, a_1=r.a)
#     @test eltype.(eachcol(rightjoin(l, r, on=:b, makeunique=true))) ==
#         [Union{Int, Missing}, CS, Int]
#
#     @test outerjoin(l, r, on=:b, makeunique=true) ≅
#         Dataset(a=vcat(l.a, fill(missing, nr)),
#                   b=vcat(l.b, r.b),
#                   a_1=vcat(fill(missing, nl), r.a))
#     @test eltype.(eachcol(outerjoin(l, r, on=:b, makeunique=true))) ==
#         [Union{Int, Missing}, CS, Union{Int, Missing}]
# end
#
# @testset "source columns" begin
#     outer_indicator = Dataset(ID = [1, 2, 2, 3, 4],
#                                 Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
#                                 Job = ["Lawyer", "Doctor", "Florist", missing, "Farmer"],
#                                 _merge = ["both", "both", "both", "left_only", "right_only"])
#
#     # Check that input data frame isn't modified (#1434)
#     pre_join_name = copy(name)
#     pre_join_job = copy(job)
#     @test outerjoin(name, job, on = :ID, source=:_merge,
#                makeunique=true) ≅
#           outerjoin(name, job, on = :ID, source="_merge",
#                makeunique=true) ≅ outer_indicator
#
#     @test name ≅ pre_join_name
#     @test job ≅ pre_join_job
#
#     # Works with conflicting names
#     name2 = Dataset(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"],
#                      _left = [1, 1, 1])
#     job2 = Dataset(ID = [1, 2, 2, 4], Job = ["Lawyer", "Doctor", "Florist", "Farmer"],
#                     _left = [1, 1, 1, 1])
#
#     outer_indicator = Dataset(ID = [1, 2, 2, 3, 4],
#                                 Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
#                                 _left = [1, 1, 1, 1, missing],
#                                 Job = ["Lawyer", "Doctor", "Florist", missing, "Farmer"],
#                                 _left_1 = [1, 1, 1, missing, 1],
#                                 _left_2 = ["both", "both", "both", "left_only", "right_only"])
#
#     @test outerjoin(name2, job2, on = :ID, source=:_left,
#                makeunique=true) ≅ outer_indicator
# end
#
# @testset "test checks of merge key uniqueness" begin
#     @test_throws ArgumentError innerjoin(name, job, on=:ID, validate=(false, true))
#     @test_throws ArgumentError innerjoin(name, job, on=:ID, validate=(true, true))
#     @test_throws ArgumentError innerjoin(job, name, on=:ID, validate=(true, false))
#     @test_throws ArgumentError innerjoin(job, name, on=:ID, validate=(true, true))
#     @test_throws ArgumentError innerjoin(job, job, on=:ID, validate=(true, true))
#
#     @test innerjoin(name, job, on=:ID, validate=(true, false)) == inner
#     @test innerjoin(name, job, on=:ID, validate=(false, false)) == inner
#
#     # Make sure ok with various special values
#     for special in [missing, NaN, -0.0]
#         name_w_special = Dataset(ID = [1, 2, 3, special],
#                                    Name = ["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
#         @test_throws ArgumentError innerjoin(name_w_special, job, on=:ID)
#         @test_throws ArgumentError leftjoin(name_w_special, job, on=:ID)
#         @test_throws ArgumentError rightjoin(name_w_special, job, on=:ID)
#         @test_throws ArgumentError outerjoin(name_w_special, job, on=:ID)
#         @test_throws ArgumentError semijoin(name_w_special, job, on=:ID)
#         @test_throws ArgumentError antijoin(name_w_special, job, on=:ID)
#     end
#
#     for special in [missing, 0.0]
#         name_w_special = Dataset(ID = [1, 2, 3, special],
#                                    Name = ["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
#         @test innerjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅ inner
#         @test leftjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅
#               vcat(left, Dataset(ID=special, Name="Maria Tester", Job=missing))
#         @test rightjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅ right
#         @test outerjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal)[[1:4;6;5], :] ≅
#               vcat(outer, Dataset(ID=special, Name="Maria Tester", Job=missing))
#         @test semijoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅ semi
#         @test antijoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅
#               vcat(anti, Dataset(ID=special, Name="Maria Tester"))
#
#         # Make sure duplicated special values still an exception
#         name_w_special_dups = Dataset(ID = [1, 2, 3, special, special],
#                                         Name = ["John Doe", "Jane Doe", "Joe Blogs",
#                                                 "Maria Tester", "Jill Jillerson"])
#         @test_throws ArgumentError innerjoin(name_w_special_dups, name, on=:ID,
#                                         validate=(true, false), matchmissing=:equal)
#     end
#
#     for special in [NaN, -0.0]
#         name_w_special = Dataset(ID = categorical([1, 2, 3, special]),
#                                    Name = ["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
#         @test innerjoin(name_w_special, transform(job, :ID => categorical => :ID), on=:ID, validate=(true, false)) == inner
#
#         # Make sure duplicated special values still an exception
#         name_w_special_dups = Dataset(ID = categorical([1, 2, 3, special, special]),
#                                         Name = ["John Doe", "Jane Doe", "Joe Blogs",
#                                                 "Maria Tester", "Jill Jillerson"])
#         @test_throws ArgumentError innerjoin(name_w_special_dups, transform(name, :ID => categorical => :ID), on=:ID,
#                                         validate=(true, false))
#     end
#
#     # Check 0.0 and -0.0 seen as different
#     name_w_zeros = Dataset(ID = categorical([1, 2, 3, 0.0, -0.0]),
#                              Name = ["John Doe", "Jane Doe",
#                                      "Joe Blogs", "Maria Tester",
#                                      "Jill Jillerson"])
#     name_w_zeros2 = Dataset(ID = categorical([1, 2, 3, 0.0, -0.0]),
#                               Name = ["John Doe", "Jane Doe",
#                                       "Joe Blogs", "Maria Tester",
#                                       "Jill Jillerson"],
#                               Name_1 = ["John Doe", "Jane Doe",
#                                         "Joe Blogs", "Maria Tester",
#                                         "Jill Jillerson"])
#
#     @test innerjoin(name_w_zeros, name_w_zeros, on=:ID, validate=(true, true),
#                makeunique=true) ≅ name_w_zeros2
#
#     # Check for multiple-column merge keys
#     name_multi = Dataset(ID1 = [1, 1, 2],
#                            ID2 = ["a", "b", "a"],
#                            Name = ["John Doe", "Jane Doe", "Joe Blogs"])
#     job_multi = Dataset(ID1 = [1, 2, 2, 4],
#                           ID2 = ["a", "b", "b", "c"],
#                           Job = ["Lawyer", "Doctor", "Florist", "Farmer"])
#     outer_multi = Dataset(ID1 = [1, 1, 2, 2, 2, 4],
#                             ID2 = ["a", "b", "a", "b", "b", "c"],
#                             Name = ["John Doe", "Jane Doe", "Joe Blogs",
#                                     missing, missing, missing],
#                             Job = ["Lawyer", missing, missing,
#                                    "Doctor", "Florist",  "Farmer"])
#
#      @test outerjoin(name_multi, job_multi, on=[:ID1, :ID2],
#                 validate=(true, false)) ≅ outer_multi
#      @test_throws ArgumentError outerjoin(name_multi, job_multi, on=[:ID1, :ID2],
#                                      validate=(false, true))
# end
#
# @testset "consistency" begin
#     # Join on symbols or vectors of symbols
#     cname = copy(name)
#     cjob = copy(job)
#     push!(cname[!, 1], cname[1, 1])
#     @test_throws AssertionError innerjoin(cname, cjob, on = :ID)
#
#     cname = copy(name)
#     cjob = copy(job)
#     push!(cjob[!, 1], cjob[1, 1])
#     @test_throws AssertionError innerjoin(cname, cjob, on = :ID)
#
#     cname = copy(name)
#     push!(Datasets._columns(cname), cname[:, 1])
#     @test_throws AssertionError innerjoin(cname, cjob, on = :ID)
# end
#
# @testset "multi data frame join" begin
#     ds1 = Dataset(id=[1, 2, 3], x=[1, 2, 3])
#     ds2 = Dataset(id=[1, 2, 4], y=[1, 2, 4])
#     ds3 = Dataset(id=[1, 3, 4], z=[1, 3, 4])
#     @test innerjoin(ds1, ds2, ds3, on=:id) == Dataset(id=1, x=1, y=1, z=1)
#     @test outerjoin(ds1, ds2, ds3, on=:id) ≅ Dataset(id=[1, 3, 4, 2],
#                                                        x=[1, 3, missing, 2],
#                                                        y=[1, missing, 4, 2],
#                                                        z=[1, 3, 4, missing])
#     @test_throws MethodError leftjoin(ds1, ds2, ds3, on=:id)
#     @test_throws MethodError rightjoin(ds1, ds2, ds3, on=:id)
#     @test_throws MethodError semijoin(ds1, ds2, ds3, on=:id)
#     @test_throws MethodError antijoin(ds1, ds2, ds3, on=:id)
#
#     dsc = crossjoin(ds1, ds2, ds3, makeunique=true)
#     @test dsc.x == dsc.id == repeat(1:3, inner=9)
#     @test dsc.y == dsc.id_1 == repeat([1, 2, 4], inner=3, outer=3)
#     @test dsc.z == dsc.id_2 == repeat([1, 3, 4], outer=9)
#
#     ds3[1, 1] = 4
#     @test_throws ArgumentError innerjoin(ds1, ds2, ds3, on=:id, validate=(true, true))
# end
#
# @testset "flexible on in join" begin
#     ds1 = Dataset(id=[1, 2, 3], id2=[11, 12, 13], x=[1, 2, 3])
#     ds2 = Dataset(id=[1, 2, 4], ID2=[11, 12, 14], y=[1, 2, 4])
#     @test innerjoin(ds1, ds2, on=[:id, :id2=>:ID2]) == Dataset(id=[1, 2], id2=[11, 12],
#                                                                  x=[1, 2], y=[1, 2])
#     @test innerjoin(ds1, ds2, on=[:id2=>:ID2, :id]) == Dataset(id=[1, 2], id2=[11, 12],
#                                                                  x=[1, 2], y=[1, 2])
#     @test innerjoin(ds1, ds2, on=[:id=>:id, :id2=>:ID2]) == Dataset(id=[1, 2], id2=[11, 12],
#                                                                       x=[1, 2], y=[1, 2])
#     @test innerjoin(ds1, ds2, on=[:id2=>:ID2, :id=>:id]) == Dataset(id=[1, 2], id2=[11, 12],
#                                                                       x=[1, 2], y=[1, 2])
# end
#
# @testset "check naming of source" begin
#     ds = Dataset(a=1)
#     @test_throws ArgumentError outerjoin(ds, ds, on=:a, source=:a)
#     @test outerjoin(ds, ds, on=:a, source=:a, makeunique=true) == Dataset(a=1, a_1="both")
#     @test outerjoin(ds, ds, on=:a, source="_left") == Dataset(a=1, _left="both")
#     @test outerjoin(ds, ds, on=:a, source="_right") == Dataset(a=1, _right="both")
#
#     ds = Dataset(_left=1)
#     @test outerjoin(ds, ds, on=:_left, source="_leftX") == Dataset(_left=1, _leftX="both")
#     ds = Dataset(_right=1)
#     @test outerjoin(ds, ds, on=:_right, source="_rightX") == Dataset(_right=1, _rightX="both")
# end
#
# @testset "validate error message composition" begin
#     for validate in ((true, false), (false, true), (true, true)),
#         a in ([1; 1], [1:2; 1:2], [1:3; 1:3]),
#         on in ([:a], [:a, :b])
#         ds = Dataset(a=a, b=1, c=1)
#         @test_throws ArgumentError outerjoin(ds, ds, on=on, validate=validate)
#     end
#     for validate in ((true, false), (false, true), (true, true)),
#         a in ([1; 1], [1:2; 1:2], [1:3; 1:3]),
#         on in ([:a=>:d], [:a => :d, :b])
#         ds1 = Dataset(a=a, b=1, c=1)
#         ds2 = Dataset(d=a, b=1, c=1)
#         @test_throws ArgumentError outerjoin(ds1, ds2, on=on, validate=validate)
#     end
#
#     # make sure we do not error when we should not
#     for validate in ((false, false), (true, false), (false, true), (true, true))
#         ds1 = Dataset(a=1, b=1)
#         ds2 = Dataset(d=1, b=1)
#         @test outerjoin(ds1, ds1, on=[:a, :b], validate=validate) == ds1
#         @test outerjoin(ds1, ds2, on=[:a => :d, :b], validate=validate) == ds1
#     end
#     ds1 = Dataset(a=[1, 1], b=1)
#     ds2 = Dataset(d=1, b=1)
#     @test outerjoin(ds1, ds2, on=[:a => :d, :b], validate=(false, true)) == ds1
#     ds1 = Dataset(a=1, b=1)
#     ds2 = Dataset(d=[1, 1], b=1)
#     @test outerjoin(ds1, ds2, on=[:a => :d, :b], validate=(true, false)) == [ds1; ds1]
#     ds1 = Dataset(a=[1, 1], b=1)
#     ds2 = Dataset(d=[1, 1], b=1)
#     @test outerjoin(ds1, ds2, on=[:a => :d, :b], validate=(false, false)) == [ds1; ds1]
# end
#
# @testset "renamecols tests" begin
#     ds1 = Dataset(id1=[1, 2, 3], id2=[1, 2, 3], x=1:3)
#     ds2 = Dataset(id1=[1, 2, 4], ID2=[1, 2, 4], x=1:3)
#
#     @test_throws ArgumentError innerjoin(ds1, ds2, on=:id1, renamecols=1=>1, makeunique=true)
#     @test_throws ArgumentError leftjoin(ds1, ds2, on=:id1, renamecols=1=>1, makeunique=true)
#     @test_throws ArgumentError rightjoin(ds1, ds2, on=:id1, renamecols=1=>1, makeunique=true)
#     @test_throws ArgumentError outerjoin(ds1, ds2, on=:id1, renamecols=1=>1, makeunique=true)
#
#     @test_throws ArgumentError innerjoin(ds1, ds2, on=:id1)
#     @test innerjoin(ds1, ds2, on=:id1, makeunique=true) ==
#         Dataset(id1=[1, 2], id2=[1, 2], x=[1, 2], ID2=[1, 2], x_1=[1, 2])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test innerjoin(ds1, ds2, on=:id1,
#                         makeunique = mu, validate = vl => vr, renamecols = l => r) ==
#             Dataset(id1=[1, 2], id2_left=[1, 2], x_left=[1, 2], ID2_right=[1, 2], x_right=[1, 2])
#     end
#
#     @test_throws ArgumentError innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2])
#     @test innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], makeunique=true) ==
#         Dataset(id1=[1, 2], id2=[1, 2], x=[1, 2], x_1=[1, 2])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                         makeunique = mu, validate = vl => vr, renamecols = l => r) ==
#             Dataset(id1=[1, 2], id2=[1, 2], x_left=[1, 2], x_right=[1, 2])
#     end
#
#     @test_throws ArgumentError leftjoin(ds1, ds2, on=:id1)
#     @test leftjoin(ds1, ds2, on=:id1, makeunique=true) ≅
#         Dataset(id1=[1, 2, 3], id2=[1, 2, 3], x=[1, 2, 3], ID2=[1, 2, missing], x_1=[1, 2, missing])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test leftjoin(ds1, ds2, on=:id1,
#                        makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
#             Dataset(id1=[1, 2, 3], id2_left=[1, 2, 3], x_left=[1, 2, 3],
#                       ID2_right=[1, 2, missing], x_right=[1, 2, missing])
#     end
#
#     @test_throws ArgumentError leftjoin(ds1, ds2, on=[:id1, :id2 => :ID2])
#     @test leftjoin(ds1, ds2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
#         Dataset(id1=[1, 2, 3], id2=[1, 2, 3], x=[1, 2, 3], x_1=[1, 2, missing])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test leftjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                        makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
#             Dataset(id1=[1, 2, 3], id2=[1, 2, 3], x_left=[1, 2, 3], x_right=[1, 2, missing])
#     end
#
#     @test_throws ArgumentError leftjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                                         renamecols = "_left" => "_right", source=:id1)
#     @test_throws ArgumentError leftjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                                         renamecols = "_left" => "_right", source=:x_left)
#     @test leftjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                    renamecols = "_left" => "_right", source=:ind) ≅
#           Dataset(id1=[1, 2, 3], id2=[1, 2, 3], x_left=[1, 2, 3],
#                     x_right=[1, 2, missing], ind=["both", "both", "left_only"])
#
#     @test_throws ArgumentError rightjoin(ds1, ds2, on=:id1)
#     @test rightjoin(ds1, ds2, on=:id1, makeunique=true) ≅
#         Dataset(id1=[1, 2, 4], id2=[1, 2, missing], x=[1, 2, missing], ID2=[1, 2, 4], x_1=[1, 2, 3])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test rightjoin(ds1, ds2, on=:id1,
#                        makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
#             Dataset(id1=[1, 2, 4], id2_left=[1, 2, missing], x_left=[1, 2, missing],
#                       ID2_right=[1, 2, 4], x_right=[1, 2, 3])
#     end
#
#     @test_throws ArgumentError rightjoin(ds1, ds2, on=[:id1, :id2 => :ID2])
#     @test rightjoin(ds1, ds2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
#         Dataset(id1=[1, 2, 4], id2=[1, 2, 4], x=[1, 2, missing], x_1=[1, 2, 3])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test rightjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                        makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
#             Dataset(id1=[1, 2, 4], id2=[1, 2, 4], x_left=[1, 2, missing], x_right=[1, 2, 3])
#     end
#
#     @test_throws ArgumentError rightjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                                          renamecols = "_left" => "_right", source=:id1)
#     @test_throws ArgumentError rightjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                                          renamecols = "_left" => "_right", source=:x_left)
#     @test rightjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                     renamecols = "_left" => "_right", source=:ind) ≅
#           Dataset(id1=[1, 2, 4], id2=[1, 2, 4], x_left=[1, 2, missing],
#                     x_right=[1, 2, 3], ind=["both", "both", "right_only"])
#
#     @test_throws ArgumentError outerjoin(ds1, ds2, on=:id1)
#     @test outerjoin(ds1, ds2, on=:id1, makeunique=true) ≅
#         Dataset(id1=[1, 2, 3, 4], id2=[1, 2, 3, missing], x=[1, 2, 3, missing],
#                   ID2=[1, 2, missing, 4], x_1=[1, 2, missing, 3])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test outerjoin(ds1, ds2, on=:id1,
#                        makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
#             Dataset(id1=[1, 2, 3, 4], id2_left=[1, 2, 3, missing], x_left=[1, 2, 3, missing],
#                       ID2_right=[1, 2, missing, 4], x_right=[1, 2, missing, 3])
#     end
#
#     @test_throws ArgumentError outerjoin(ds1, ds2, on=[:id1, :id2 => :ID2])
#     @test outerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
#         Dataset(id1=[1, 2, 3, 4], id2=[1, 2, 3, 4], x=[1, 2, 3, missing], x_1=[1, 2, missing, 3])
#     for l in ["_left", :_left, x -> x * "_left"],
#         r in ["_right", :_right, x -> x * "_right"],
#         mu in [true, false], vl in [true, false], vr in [true, false]
#         @test outerjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                        makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
#             Dataset(id1=[1, 2, 3, 4], id2=[1, 2, 3, 4], x_left=[1, 2, 3, missing], x_right=[1, 2, missing, 3])
#     end
#
#     @test_throws ArgumentError outerjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                                          renamecols = "_left" => "_right", source=:id1)
#     @test_throws ArgumentError outerjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                                          renamecols = "_left" => "_right", source=:x_left)
#     @test outerjoin(ds1, ds2, on=[:id1, :id2 => :ID2],
#                     renamecols = "_left" => "_right", source=:ind) ≅
#           Dataset(id1=[1, 2, 3, 4], id2=[1, 2, 3, 4], x_left=[1, 2, 3, missing],
#                     x_right=[1, 2, missing, 3], ind=["both", "both", "left_only", "right_only"])
#
#     ds1.x .+= 10
#     ds2.x .+= 100
#     @test_throws ArgumentError innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = (x -> :id1) => "_right")
#     @test innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = (x -> :id1) => "_right", makeunique=true) ==
#           Dataset(id1=1:2, id2=1:2, id1_1=11:12, x_right=101:102)
#     @test_throws ArgumentError innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :id2))
#     @test innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :id2), makeunique=true) ==
#           Dataset(id1=1:2, id2=1:2, x_left=11:12, id2_1=101:102)
#     @test_throws ArgumentError innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = "_left" => "_left")
#     @test innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = "_left" => "_left", makeunique=true) ==
#           Dataset(id1=1:2, id2=1:2, x_left=11:12, x_left_1=101:102)
#     ds2.y = ds2.x .+ 1
#     @test_throws ArgumentError innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :newcol))
#     @test innerjoin(ds1, ds2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :newcol), makeunique=true) ==
#           Dataset(id1=1:2, id2=1:2, x_left=11:12, newcol=101:102, newcol_1=102:103)
# end
#
# @testset "careful source test" begin
#     Random.seed!(1234)
#     for i in 5:15, j in 5:15
#         ds1 = Dataset(id=rand(1:10, i), x=1:i)
#         ds2 = Dataset(id=rand(1:10, j), y=1:j)
#         dsi = innerjoin(ds1, ds2, on=:id)
#         dsl = leftjoin(ds1, ds2, on=:id, source=:ind)
#         dsr = rightjoin(ds1, ds2, on=:id, source=:ind)
#         dso = outerjoin(ds1, ds2, on=:id, source=:ind)
#         @test issorted(dsl, :ind)
#         @test issorted(dsr, :ind)
#         @test issorted(dso, :ind)
#
#         @test all(==("both"), dsl[1:nrow(dsi), :ind])
#         @test dsl[1:nrow(dsi), 1:3] ≅ dsi
#         @test all(==("left_only"), dsl[nrow(dsi)+1:end, :ind])
#
#         @test all(==("both"), dsr[1:nrow(dsi), :ind])
#         @test dsr[1:nrow(dsi), 1:3] ≅ dsi
#         @test all(==("right_only"), dsr[nrow(dsi)+1:end, :ind])
#
#         @test all(==("both"), dso[1:nrow(dsi), :ind])
#         @test dsl ≅ dso[1:nrow(dsl), :]
#         @test all(==("right_only"), dso[nrow(dsl)+1:end, :ind])
#     end
# end
#
# @testset "removed join function" begin
#     ds1 = Dataset(id=[1, 2, 3], x=[1, 2, 3])
#     ds2 = Dataset(id=[1, 2, 4], y=[1, 2, 4])
#     ds3 = Dataset(id=[1, 3, 4], z=[1, 3, 4])
#     @test_throws ArgumentError join(ds1, ds2, ds3, on=:id, kind=:left)
#     @test_throws ArgumentError join(ds1, ds2, on=:id, kind=:inner)
# end
#
# @testset "join mixing Dataset and SubDataset" begin
#     ds1 = Dataset(a=[1, 2, 3], b=[4, 5, 6])
#     ds1_copy = ds1[ds1.a .> 1, :]
#     ds1_view1 = @view ds1[ds1.a .> 1, :]
#     ds1_view2 = @view ds1[ds1.a .> 1, 1:2]
#     ds2 = Dataset(a=[1, 2, 3], c=[7, 8, 9])
#     @test innerjoin(ds1_copy, ds2, on=:a) ==
#           innerjoin(ds1_view1, ds2, on=:a) ==
#           innerjoin(ds1_view2, ds2, on=:a)
# end
#
# @testset "OnCol correctness tests" begin
#     Random.seed!(1234)
#     c1 = collect(1:10^2)
#     c2 = collect(Float64, 1:10^2)
#     c3 = collect(sort(string.(1:10^2)))
#     c4 = repeat(1:10, inner=10)
#     c5 = collect(Float64, repeat(1:50, inner=2))
#     c6 = sort(string.(repeat(1:25,inner=4)))
#     c7 = repeat(20:-1:1, inner=5)
#
#     @test_throws AssertionError OnCol()
#     @test_throws AssertionError OnCol(c1)
#     @test_throws AssertionError OnCol(c1, [1])
#     @test_throws MethodError OnCol(c1, 1)
#
#     oncols = [OnCol(c1, c2), OnCol(c3, c4), OnCol(c5, c6), OnCol(c1, c2, c3),
#               OnCol(c2, c3, c4), OnCol(c4, c5, c6), OnCol(c1, c2, c3, c4),
#               OnCol(c2, c3, c4, c5), OnCol(c3, c4, c5, c6), OnCol(c1, c2, c3, c4, c5),
#               OnCol(c2, c3, c4, c5, c6), OnCol(c1, c2, c3, c4, c5, c6),
#               OnCol(c4, c7), OnCol(c4, c5, c7), OnCol(c4, c5, c6, c7)]
#     tupcols = [tuple.(c1, c2), tuple.(c3, c4), tuple.(c5, c6), tuple.(c1, c2, c3),
#                tuple.(c2, c3, c4), tuple.(c4, c5, c6), tuple.(c1, c2, c3, c4),
#                tuple.(c2, c3, c4, c5), tuple.(c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5),
#                tuple.(c2, c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5, c6),
#                tuple.(c4, c7), tuple.(c4, c5, c7), tuple.(c4, c5, c6, c7)]
#
#     for (oncol, tupcol) in zip(oncols, tupcols)
#         @test issorted(oncol) == issorted(tupcol)
#         @test IndexStyle(oncol) === IndexLinear()
#         @test_throws MethodError oncol[1] == oncol[2]
#     end
#
#     for i in eachindex(c1), j in eachindex(oncols, tupcols)
#         @test_throws MethodError hash(oncols[j][1], zero(UInt))
#         Datasets._prehash(oncols[j])
#         @test hash(oncols[j][i]) == hash(tupcols[j][i])
#         for k in eachindex(c1)
#             @test isequal(oncols[j][i], oncols[j][k]) == isequal(tupcols[j][i], tupcols[j][k])
#             @test isequal(oncols[j][k], oncols[j][i]) == isequal(tupcols[j][k], tupcols[j][i])
#             @test isless(oncols[j][i], oncols[j][k]) == isless(tupcols[j][i], tupcols[j][k])
#             @test isless(oncols[j][k], oncols[j][i]) == isless(tupcols[j][k], tupcols[j][i])
#         end
#     end
#
#     foreach(shuffle!, [c1, c2, c3, c4, c5, c6])
#
#     tupcols = [tuple.(c1, c2), tuple.(c3, c4), tuple.(c5, c6), tuple.(c1, c2, c3),
#                tuple.(c2, c3, c4), tuple.(c4, c5, c6), tuple.(c1, c2, c3, c4),
#                tuple.(c2, c3, c4, c5), tuple.(c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5),
#                tuple.(c2, c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5, c6),
#                tuple.(c4, c7), tuple.(c4, c5, c7), tuple.(c4, c5, c6, c7)]
#
#     for i in eachindex(c1), j in eachindex(oncols, tupcols)
#         Datasets._prehash(oncols[j])
#         @test hash(oncols[j][i]) == hash(tupcols[j][i])
#         for k in eachindex(c1)
#             @test isequal(oncols[j][i], oncols[j][k]) == isequal(tupcols[j][i], tupcols[j][k])
#             @test isequal(oncols[j][k], oncols[j][i]) == isequal(tupcols[j][k], tupcols[j][i])
#             @test isless(oncols[j][i], oncols[j][k]) == isless(tupcols[j][i], tupcols[j][k])
#             @test isless(oncols[j][k], oncols[j][i]) == isless(tupcols[j][k], tupcols[j][i])
#         end
#     end
# end
#
# @testset "join correctness tests" begin
#
#     @test_throws ArgumentError Datasets.prepare_on_col()
#
#     function test_join(ds1, ds2)
#         @assert names(ds1) == ["id", "x"]
#         @assert names(ds2) == ["id", "y"]
#
#         ds_inner = Dataset(id=[], x=[], y=[])
#         for i in axes(ds1, 1), j in axes(ds2, 1)
#             if isequal(ds1.id[i], ds2.id[j])
#                 v = ds1.id[i] isa CategoricalValue ? unwrap(ds1.id[i]) : ds1.id[i]
#                 push!(ds_inner, (id=v, x=ds1.x[i], y=ds2.y[j]))
#             end
#         end
#
#         ds_left_part = Dataset(id=[], x=[], y=[])
#         for i in axes(ds1, 1)
#             if !(ds1.id[i] in Set(ds2.id))
#                 v = ds1.id[i] isa CategoricalValue ? unwrap(ds1.id[i]) : ds1.id[i]
#                 push!(ds_left_part, (id=v, x=ds1.x[i], y=missing))
#             end
#         end
#
#         ds_right_part = Dataset(id=[], x=[], y=[])
#         for i in axes(ds2, 1)
#             if !(ds2.id[i] in Set(ds1.id))
#                 v = ds2.id[i] isa CategoricalValue ? unwrap(ds2.id[i]) : ds2.id[i]
#                 push!(ds_right_part, (id=v, x=missing, y=ds2.y[i]))
#             end
#         end
#
#         ds_left = vcat(ds_inner, ds_left_part)
#         ds_right = vcat(ds_inner, ds_right_part)
#         ds_outer = vcat(ds_inner, ds_left_part, ds_right_part)
#
#         ds_semi = ds1[[x in Set(ds2.id) for x in ds1.id], :]
#         ds_anti = ds1[[!(x in Set(ds2.id)) for x in ds1.id], :]
#
#         ds1x = copy(ds1)
#         ds1x.id2 = copy(ds1x.id)
#         ds2x = copy(ds2)
#         ds2x.id2 = copy(ds2x.id)
#
#         ds1x2 = copy(ds1x)
#         ds1x2.id3 = copy(ds1x2.id)
#         ds2x2 = copy(ds2x)
#         ds2x2.id3 = copy(ds2x2.id)
#
#         sort!(ds_inner, [:x, :y])
#         sort!(ds_left, [:x, :y])
#         sort!(ds_right, [:x, :y])
#         sort!(ds_outer, [:x, :y])
#
#         ds_inner2 = copy(ds_inner)
#         ds_left2 = copy(ds_left)
#         ds_right2 = copy(ds_right)
#         ds_outer2 = copy(ds_outer)
#         ds_semi2 = copy(ds_semi)
#         ds_anti2 = copy(ds_anti)
#         insertcols!(ds_inner2, 3, :id2 => ds_inner2.id)
#         insertcols!(ds_left2, 3, :id2 => ds_left2.id)
#         insertcols!(ds_right2, 3, :id2 => ds_right2.id)
#         insertcols!(ds_outer2, 3, :id2 => ds_outer2.id)
#         insertcols!(ds_semi2, 3, :id2 => ds_semi2.id)
#         insertcols!(ds_anti2, 3, :id2 => ds_anti2.id)
#         ds_inner3 = copy(ds_inner2)
#         ds_left3 = copy(ds_left2)
#         ds_right3 = copy(ds_right2)
#         ds_outer3 = copy(ds_outer2)
#         ds_semi3 = copy(ds_semi2)
#         ds_anti3 = copy(ds_anti2)
#         insertcols!(ds_inner3, 4, :id3 => ds_inner3.id)
#         insertcols!(ds_left3, 4, :id3 => ds_left3.id)
#         insertcols!(ds_right3, 4, :id3 => ds_right3.id)
#         insertcols!(ds_outer3, 4, :id3 => ds_outer3.id)
#         insertcols!(ds_semi3, 4, :id3 => ds_semi3.id)
#         insertcols!(ds_anti3, 4, :id3 => ds_anti3.id)
#
#         return ds_inner ≅ sort(innerjoin(ds1, ds2, on=:id, matchmissing=:equal), [:x, :y]) &&
#                ds_inner2 ≅ sort(innerjoin(ds1x, ds2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
#                ds_inner3 ≅ sort(innerjoin(ds1x2, ds2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
#                ds_left ≅ sort(leftjoin(ds1, ds2, on=:id, matchmissing=:equal), [:x, :y]) &&
#                ds_left2 ≅ sort(leftjoin(ds1x, ds2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
#                ds_left3 ≅ sort(leftjoin(ds1x2, ds2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
#                ds_right ≅ sort(rightjoin(ds1, ds2, on=:id, matchmissing=:equal), [:x, :y]) &&
#                ds_right2 ≅ sort(rightjoin(ds1x, ds2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
#                ds_right3 ≅ sort(rightjoin(ds1x2, ds2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
#                ds_outer ≅ sort(outerjoin(ds1, ds2, on=:id, matchmissing=:equal), [:x, :y]) &&
#                ds_outer2 ≅ sort(outerjoin(ds1x, ds2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
#                ds_outer3 ≅ sort(outerjoin(ds1x2, ds2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
#                ds_semi ≅ semijoin(ds1, ds2, on=:id, matchmissing=:equal) &&
#                ds_semi2 ≅ semijoin(ds1x, ds2x, on=[:id, :id2], matchmissing=:equal) &&
#                ds_semi3 ≅ semijoin(ds1x2, ds2x2, on=[:id, :id2, :id3], matchmissing=:equal) &&
#                ds_anti ≅ antijoin(ds1, ds2, on=:id, matchmissing=:equal) &&
#                ds_anti2 ≅ antijoin(ds1x, ds2x, on=[:id, :id2], matchmissing=:equal) &&
#                ds_anti3 ≅ antijoin(ds1x2, ds2x2, on=[:id, :id2, :id3], matchmissing=:equal)
#     end
#
#     Random.seed!(1234)
#     for i in 1:5, j in 0:2
#         for ds1 in [Dataset(id=rand(1:i+j, i+j), x=1:i+j), Dataset(id=rand(1:i, i), x=1:i),
#                     Dataset(id=[rand(1:i+j, i+j); missing], x=1:i+j+1),
#                     Dataset(id=[rand(1:i, i); missing], x=1:i+1)],
#             ds2 in [Dataset(id=rand(1:i+j, i+j), y=1:i+j), Dataset(id=rand(1:i, i), y=1:i),
#                     Dataset(id=[rand(1:i+j, i+j); missing], y=1:i+j+1),
#                     Dataset(id=[rand(1:i, i); missing], y=1:i+1)]
#             for opleft = [identity, sort, x -> unique(x, :id), x -> sort(unique(x, :id))],
#                 opright = [identity, sort, x -> unique(x, :id), x -> sort(unique(x, :id))]
#
#                 # integers
#                 @test test_join(opleft(ds1), opright(ds2))
#                 @test test_join(opleft(ds1), opright(rename(ds1, :x => :y)))
#
#                 # strings
#                 ds1s = copy(ds1)
#                 ds1s[!, 1] = passmissing(string).(ds1s[!, 1])
#                 ds2s = copy(ds2)
#                 ds2s[!, 1] = passmissing(string).(ds2s[!, 1])
#                 @test test_join(opleft(ds1s), opright(ds2s))
#                 @test test_join(opleft(ds1s), opright(rename(ds1s, :x => :y)))
#
#                 # PooledArrays
#                 ds1p = copy(ds1)
#                 ds1p[!, 1] = PooledArray(ds1p[!, 1])
#                 ds2p = copy(ds2)
#                 ds2p[!, 1] = PooledArray(ds2p[!, 1])
#                 @test test_join(opleft(ds1), opright(ds2p))
#                 @test test_join(opleft(ds1p), opright(ds2))
#                 @test test_join(opleft(ds1p), opright(ds2p))
#                 @test test_join(opleft(ds1p), opright(rename(ds1p, :x => :y)))
#
#                 # add unused level
#                 ds1p[1, 1] = 0
#                 ds2p[1, 1] = 0
#                 ds1p[1, 1] = 1
#                 ds2p[1, 1] = 1
#                 @test test_join(opleft(ds1), opright(ds2p))
#                 @test test_join(opleft(ds1p), opright(ds2))
#                 @test test_join(opleft(ds1p), opright(ds2p))
#                 @test test_join(opleft(ds1p), opright(rename(ds1p, :x => :y)))
#
#                 # CategoricalArrays
#                 ds1c = copy(ds1)
#                 ds1c[!, 1] = categorical(ds1c[!, 1])
#                 ds2c = copy(ds2)
#                 ds2c[!, 1] = categorical(ds2c[!, 1])
#                 @test test_join(opleft(ds1), opright(ds2c))
#                 @test test_join(opleft(ds1c), opright(ds2c))
#                 @test test_join(opleft(ds1c), opright(ds2))
#                 @test test_join(opleft(ds1c), opright(rename(ds1c, :x => :y)))
#                 @test test_join(opleft(ds1p), opright(ds2c))
#                 @test test_join(opleft(ds1c), opright(ds2p))
#
#                 # add unused level
#                 ds1c[1, 1] = 0
#                 ds2c[1, 1] = 0
#                 ds1c[1, 1] = 1
#                 ds2c[1, 1] = 1
#                 @test test_join(opleft(ds1), opright(ds2c))
#                 @test test_join(opleft(ds1c), opright(ds2c))
#                 @test test_join(opleft(ds1c), opright(ds2))
#                 @test test_join(opleft(ds1c), opright(rename(ds1c, :x => :y)))
#                 @test test_join(opleft(ds1p), opright(ds2c))
#                 @test test_join(opleft(ds1c), opright(ds2p))
#             end
#         end
#     end
#
#     # some special cases
#     @test isequal_coltyped(innerjoin(Dataset(id=[]), Dataset(id=[]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[]), Dataset(id=[]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[]), Dataset(id=[]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[]), Dataset(id=[]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(semijoin(Dataset(id=[]), Dataset(id=[]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(antijoin(Dataset(id=[]), Dataset(id=[]), on=:id),
#                            Dataset(id=[]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=[1, 2, 3]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=Any[1, 2, 3]))
#     @test isequal_coltyped(semijoin(Dataset(id=[]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=[]))
#     @test isequal_coltyped(antijoin(Dataset(id=[]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=[]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[1, 2, 3]), Dataset(id=[]), on=:id),
#                            Dataset(id=Int[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[1, 2, 3]), Dataset(id=[]), on=:id),
#                            Dataset(id=[1, 2, 3]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[1, 2, 3]), Dataset(id=[]), on=:id),
#                            Dataset(id=Any[]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[1, 2, 3]), Dataset(id=[]), on=:id),
#                            Dataset(id=Any[1, 2, 3]))
#     @test isequal_coltyped(semijoin(Dataset(id=[1, 2, 3]), Dataset(id=[]), on=:id),
#                            Dataset(id=Int[]))
#     @test isequal_coltyped(antijoin(Dataset(id=[1, 2, 3]), Dataset(id=[]), on=:id),
#                            Dataset(id=[1, 2, 3]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[4, 5, 6]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=Int[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[4, 5, 6]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=Int[4, 5, 6]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[4, 5, 6]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=Int[1, 2, 3]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[4, 5, 6]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=Int[4, 5, 6, 1, 2, 3]))
#     @test isequal_coltyped(semijoin(Dataset(id=[4, 5, 6]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=Int[]))
#     @test isequal_coltyped(antijoin(Dataset(id=[4, 5, 6]), Dataset(id=[1, 2, 3]), on=:id),
#                            Dataset(id=[4, 5, 6]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[1, 2, 3]), Dataset(id=[4, 5, 6]), on=:id),
#                            Dataset(id=Int[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[1, 2, 3]), Dataset(id=[4, 5, 6]), on=:id),
#                            Dataset(id=Int[1, 2, 3]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[1, 2, 3]), Dataset(id=[4, 5, 6]), on=:id),
#                            Dataset(id=Int[4, 5, 6]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[1, 2, 3]), Dataset(id=[4, 5, 6]), on=:id),
#                            Dataset(id=Int[1, 2, 3, 4, 5, 6]))
#     @test isequal_coltyped(semijoin(Dataset(id=[1, 2, 3]), Dataset(id=[4, 5, 6]), on=:id),
#                            Dataset(id=Int[]))
#     @test isequal_coltyped(antijoin(Dataset(id=[1, 2, 3]), Dataset(id=[4, 5, 6]), on=:id),
#                            Dataset(id=[1, 2, 3]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[missing]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Missing[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[missing]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=[missing]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[missing]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=[1]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[missing]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=[missing, 1]))
#     @test isequal_coltyped(semijoin(Dataset(id=[missing]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Missing[]))
#     @test isequal_coltyped(antijoin(Dataset(id=[missing]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=[missing]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=Missing[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Missing[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=Missing[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Missing[]))
#     @test isequal_coltyped(rightjoin(Dataset(id=Missing[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=[1]))
#     @test isequal_coltyped(outerjoin(Dataset(id=Missing[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[1]))
#     @test isequal_coltyped(semijoin(Dataset(id=Missing[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Missing[]))
#     @test isequal_coltyped(antijoin(Dataset(id=Missing[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Missing[]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(rightjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=[1]))
#     @test isequal_coltyped(outerjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[1]))
#     @test isequal_coltyped(semijoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(antijoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[1]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[2, 1, 2]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[2, 1, 2]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(rightjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[2, 1, 2]), on=:id, matchmissing=:equal),
#                            Dataset(id=[2, 1, 2]))
#     @test isequal_coltyped(outerjoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[2, 1, 2]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[2, 1, 2]))
#     @test isequal_coltyped(semijoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[2, 1, 2]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(antijoin(Dataset(id=Union{Int, Missing}[]), Dataset(id=[2, 1, 2]), on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(leftjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1]),
#                                     on=:id, matchmissing=:equal) ,
#                            Dataset(id=Union{Int, Missing}[missing]))
#     @test isequal_coltyped(rightjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[1]))
#     @test isequal_coltyped(outerjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[missing, 1]))
#     @test isequal_coltyped(semijoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#     @test isequal_coltyped(antijoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[missing]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[missing]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[missing]), Dataset(id=[1, missing]),
#                                     on=:id, matchmissing=:equal),
#                            Dataset(id=[missing]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[missing, 1]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[missing, 1]))
#     @test isequal_coltyped(semijoin(Dataset(id=[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[missing]))
#     @test isequal_coltyped(antijoin(Dataset(id=[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=Missing[]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[missing]))
#     @test isequal_coltyped(leftjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1, missing]),
#                                     on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[missing]))
#     @test isequal_coltyped(rightjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[missing, 1]))
#     @test isequal_coltyped(outerjoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=[missing, 1]))
#     @test isequal_coltyped(semijoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[missing]))
#     @test isequal_coltyped(antijoin(Dataset(id=Union{Int, Missing}[missing]), Dataset(id=[1, missing]),
#                                      on=:id, matchmissing=:equal),
#                            Dataset(id=Union{Int, Missing}[]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[typemin(Int) + 1, typemin(Int)]), Dataset(id=[typemin(Int)]), on=:id),
#                            Dataset(id=[typemin(Int)]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[typemin(Int) + 1, typemin(Int)]), Dataset(id=[typemin(Int)]), on=:id),
#                            Dataset(id=[typemin(Int), typemin(Int) + 1]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[typemin(Int) + 1, typemin(Int)]), Dataset(id=[typemin(Int)]), on=:id),
#                            Dataset(id=[typemin(Int)]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[typemin(Int) + 1, typemin(Int)]), Dataset(id=[typemin(Int)]), on=:id),
#                            Dataset(id=[typemin(Int), typemin(Int) + 1]))
#     @test isequal_coltyped(semijoin(Dataset(id=[typemin(Int) + 1, typemin(Int)]), Dataset(id=[typemin(Int)]), on=:id),
#                            Dataset(id=[typemin(Int)]))
#     @test isequal_coltyped(antijoin(Dataset(id=[typemin(Int) + 1, typemin(Int)]), Dataset(id=[typemin(Int)]), on=:id),
#                            Dataset(id=[typemin(Int) + 1]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[typemax(Int), typemax(Int) - 1]), Dataset(id=[typemax(Int)]), on=:id),
#                            Dataset(id=[typemax(Int)]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[typemax(Int), typemax(Int) - 1]), Dataset(id=[typemax(Int)]), on=:id),
#                            Dataset(id=[typemax(Int), typemax(Int) - 1]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[typemax(Int), typemax(Int) - 1]), Dataset(id=[typemax(Int)]), on=:id),
#                            Dataset(id=[typemax(Int)]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[typemax(Int), typemax(Int) - 1]), Dataset(id=[typemax(Int)]), on=:id),
#                            Dataset(id=[typemax(Int), typemax(Int) - 1]))
#     @test isequal_coltyped(semijoin(Dataset(id=[typemax(Int), typemax(Int) - 1]), Dataset(id=[typemax(Int)]), on=:id),
#                            Dataset(id=[typemax(Int)]))
#     @test isequal_coltyped(antijoin(Dataset(id=[typemax(Int), typemax(Int) - 1]), Dataset(id=[typemax(Int)]), on=:id),
#                            Dataset(id=[typemax(Int) - 1]))
#
#     @test isequal_coltyped(innerjoin(Dataset(id=[2000, 2, 100]), Dataset(id=[2000, 1, 100]), on=:id),
#                            Dataset(id=[2000, 100]))
#     @test isequal_coltyped(leftjoin(Dataset(id=[2000, 2, 100]), Dataset(id=[2000, 1, 100]), on=:id),
#                            Dataset(id=[2000, 100, 2]))
#     @test isequal_coltyped(rightjoin(Dataset(id=[2000, 2, 100]), Dataset(id=[2000, 1, 100]), on=:id),
#                            Dataset(id=[2000, 100, 1]))
#     @test isequal_coltyped(outerjoin(Dataset(id=[2000, 2, 100]), Dataset(id=[2000, 1, 100]), on=:id),
#                            Dataset(id=[2000, 100, 2, 1]))
#     @test isequal_coltyped(semijoin(Dataset(id=[2000, 2, 100]), Dataset(id=[2000, 1, 100]), on=:id),
#                            Dataset(id=[2000, 100]))
#     @test isequal_coltyped(antijoin(Dataset(id=[2000, 2, 100]), Dataset(id=[2000, 1, 100]), on=:id),
#                            Dataset(id=[2]))
#
#     @test isequal_coltyped(outerjoin(Dataset(id=[1]), Dataset(id=[4.5]), on=:id),
#                            Dataset(id=[1, 4.5]))
#     @test isequal_coltyped(outerjoin(Dataset(id=categorical([1])), Dataset(id=[(1, 2)]), on=:id),
#                            Dataset(id=[1, (1, 2)]))
# end
#
# @testset "legacy merge tests" begin
#     Random.seed!(1)
#     ds1 = Dataset(a = shuffle!(Vector{Union{Int, Missing}}(1:10)),
#                     b = rand(Union{Symbol, Missing}[:A, :B], 10),
#                     v1 = Vector{Union{Float64, Missing}}(randn(10)))
#
#     ds2 = Dataset(a = shuffle!(Vector{Union{Int, Missing}}(1:5)),
#                     b2 = rand(Union{Symbol, Missing}[:A, :B, :C], 5),
#                     v2 = Vector{Union{Float64, Missing}}(randn(5)))
#
#     m1 = innerjoin(ds1, ds2, on = :a)
#     @test m1[!, :a] == ds1[!, :a][ds1[!, :a] .<= 5] # preserves ds1 order
#     m2 = outerjoin(ds1, ds2, on = :a)
#     @test m2[!, :a] != ds1[!, :a] # does not preserve ds1 order
#     @test m2[!, :b] != ds1[!, :b] # does not preserve ds1 order
#     @test sort(m2[!, [:a, :b]]) == sort(ds1[!, [:a, :b]]) # but keeps values
#     @test m1 == m2[1:nrow(m1), :] # and is consistent with innerjoin in the first rows
#     @test m2[indexin(ds1[!, :a], m2[!, :a]), :b] == ds1[!, :b]
#     @test m2[indexin(ds2[!, :a], m2[!, :a]), :b2] == ds2[!, :b2]
#     @test m2[indexin(ds1[!, :a], m2[!, :a]), :v1] == ds1[!, :v1]
#     @test m2[indexin(ds2[!, :a], m2[!, :a]), :v2] == ds2[!, :v2]
#     @test all(ismissing, m2[map(x -> !in(x, ds2[!, :a]), m2[!, :a]), :b2])
#     @test all(ismissing, m2[map(x -> !in(x, ds2[!, :a]), m2[!, :a]), :v2])
#
#     ds1 = Dataset(a = Union{Int, Missing}[1, 2, 3],
#                     b = Union{String, Missing}["America", "Europe", "Africa"])
#     ds2 = Dataset(a = Union{Int, Missing}[1, 2, 4],
#                     c = Union{String, Missing}["New World", "Old World", "New World"])
#
#     m1 = innerjoin(ds1, ds2, on = :a)
#     @test m1[!, :a] == [1, 2]
#
#     m2 = leftjoin(ds1, ds2, on = :a)
#     @test m2[!, :a] == [1, 2, 3]
#
#     m3 = rightjoin(ds1, ds2, on = :a)
#     @test m3[!, :a] == [1, 2, 4]
#
#     m4 = outerjoin(ds1, ds2, on = :a)
#     @test m4[!, :a] == [1, 2, 3, 4]
#
#     # test with missings (issue #185)
#     ds1 = Dataset()
#     ds1[!, :A] = ["a", "b", "a", missing]
#     ds1[!, :B] = Union{Int, Missing}[1, 2, 1, 3]
#
#     ds2 = Dataset()
#     ds2[!, :A] = ["a", missing, "c"]
#     ds2[!, :C] = Union{Int, Missing}[1, 2, 4]
#
#     @test_throws ArgumentError innerjoin(ds1, ds2, on = :A)
#     m1 = innerjoin(ds1, ds2, on = :A, matchmissing=:equal)
#     @test size(m1) == (3, 3)
#     @test m1[!, :A] ≅ ["a", "a", missing]
#
#     @test_throws ArgumentError outerjoin(ds1, ds2, on = :A)
#     m2 = outerjoin(ds1, ds2, on = :A, matchmissing=:equal)
#     @test size(m2) == (5, 3)
#     @test m2[!, :A] ≅ ["a", "a", missing, "b", "c"]
# end
#
# @testset "legacy join tests" begin
#     ds1 = Dataset(a = Union{Symbol, Missing}[:x, :y][[1, 1, 1, 2, 1, 1]],
#                     b = Union{Symbol, Missing}[:A, :B, :D][[1, 1, 2, 2, 1, 3]],
#                     v1 = 1:6)
#
#     ds2 = Dataset(a = Union{Symbol, Missing}[:x, :y][[2, 2, 1, 1, 1, 1]],
#                     b = Union{Symbol, Missing}[:A, :B, :C][[1, 2, 1, 2, 3, 1]],
#                     v2 = 1:6)
#     ds2[1, :a] = missing
#
#     m1 = innerjoin(ds1, ds2, on = [:a, :b], matchmissing=:equal)
#     @test sort(m1) == sort(Dataset(a=[:x, :x, :x, :x, :x, :y, :x, :x],
#                                      b=[:A, :A, :A, :A, :B, :B, :A, :A],
#                                      v1=[1, 1, 2, 2, 3, 4, 5, 5],
#                                      v2=[3, 6, 3, 6, 4, 2, 3, 6]))
#     m2 = outerjoin(ds1, ds2, on = [:a, :b], matchmissing=:equal)
#     @test sort(m2) ≅ sort(Dataset(a=[:x, :x, :x, :x, :x, :y, :x, :x, :x, missing, :x],
#                                     b=[:A, :A, :A, :A, :B, :B, :A, :A, :D, :A, :C],
#                                     v1=[1, 1, 2, 2, 3, 4, 5, 5, 6, missing, missing],
#                                     v2=[3, 6, 3, 6, 4, 2, 3, 6, missing, 1, 5]))
#
#     Random.seed!(1)
#     ds1 = Dataset(a = ["abc", "abx", "axz", "def", "dsr"], v1 = randn(5))
#     ds2 = Dataset(a = ["def", "abc", "abx", "axz", "xyz"], v2 = randn(5))
#     transform!(ds1, :a => ByRow(collect) => AsTable)
#     transform!(ds2, :a => ByRow(collect) => AsTable)
#
#     m1 = innerjoin(ds1, ds2, on = :a, makeunique=true)
#     m2 = innerjoin(ds1, ds2, on = [:x1, :x2, :x3], makeunique=true)
#     @test m1[!, :a] == m2[!, :a]
# end
#
# @testset "threaded correctness" begin
#     ds1 = Dataset(id=[1:10^6; 10^7+1:10^7+2])
#     ds1.left_row = axes(ds1, 1)
#     ds2 = Dataset(id=[1:10^6; 10^8+1:10^8+4])
#     ds2.right_row = axes(ds2, 1)
#
#     ds_inner = Dataset(id=1:10^6, left_row=1:10^6, right_row=1:10^6)
#     ds_left = Dataset(id=[1:10^6; 10^7+1:10^7+2], left_row=1:10^6+2,
#                         right_row=[1:10^6; missing; missing])
#     ds_right = Dataset(id=[1:10^6; 10^8+1:10^8+4],
#                          left_row=[1:10^6; fill(missing, 4)],
#                          right_row=1:10^6+4)
#     ds_outer = Dataset(id=[1:10^6; 10^7+1:10^7+2; 10^8+1:10^8+4],
#                          left_row=[1:10^6+2; fill(missing, 4)],
#                          right_row=[1:10^6; missing; missing; 10^6+1:10^6+4])
#     ds_semi = Dataset(id=1:10^6, left_row=1:10^6)
#     ds_anti = Dataset(id=10^7+1:10^7+2, left_row=10^6+1:10^6+2)
#
#     @test innerjoin(ds1, ds2, on=:id) ≅ ds_inner
#     @test leftjoin(ds1, ds2, on=:id) ≅ ds_left
#     @test rightjoin(ds1, ds2, on=:id) ≅ ds_right
#     @test outerjoin(ds1, ds2, on=:id) ≅ ds_outer
#     @test semijoin(ds1, ds2, on=:id) ≅ ds_semi
#     @test antijoin(ds1, ds2, on=:id) ≅ ds_anti
#
#     Random.seed!(1234)
#     for i in 1:4
#         ds1 = ds1[shuffle(axes(ds1, 1)), :]
#         ds2 = ds2[shuffle(axes(ds2, 1)), :]
#         @test sort!(innerjoin(ds1, ds2, on=:id)) ≅ ds_inner
#         @test sort!(leftjoin(ds1, ds2, on=:id)) ≅ ds_left
#         @test sort!(rightjoin(ds1, ds2, on=:id)) ≅ ds_right
#         @test sort!(outerjoin(ds1, ds2, on=:id)) ≅ ds_outer
#         @test sort!(semijoin(ds1, ds2, on=:id)) ≅ ds_semi
#         @test sort!(antijoin(ds1, ds2, on=:id)) ≅ ds_anti
#     end
#
#     # test correctness of column order
#     ds1 = Dataset(a="a", id2=-[1:10^6; 10^7+1:10^7+2], b="b",
#                     id1=[1:10^6; 10^7+1:10^7+2], c="c", d="d")
#     ds2 = Dataset(e="e", id1=[1:10^6; 10^8+1:10^8+4], f="f", g="g",
#                     id2=-[1:10^6; 10^8+1:10^8+4], h="h")
#     @test innerjoin(ds1, ds2, on=[:id1, :id2]) ≅
#           Dataset(a="a", id2=-(1:10^6), b="b", id1=1:10^6,
#                     c="c", d="d", e="e", f="f", g="g", h="h")
#     @test leftjoin(ds1, ds2, on=[:id1, :id2])[1:10^6, :] ≅
#           Dataset(a="a", id2=-(1:10^6), b="b", id1=1:10^6,
#                     c="c", d="d", e="e", f="f", g="g", h="h")
#     @test rightjoin(ds1, ds2, on=[:id1, :id2])[1:10^6, :] ≅
#           Dataset(a="a", id2=-(1:10^6), b="b", id1=1:10^6,
#                     c="c", d="d", e="e", f="f", g="g", h="h")
#     @test outerjoin(ds1, ds2, on=[:id1, :id2])[1:10^6, :] ≅
#           Dataset(a="a", id2=-(1:10^6), b="b", id1=1:10^6,
#                     c="c", d="d", e="e", f="f", g="g", h="h")
#     @test semijoin(ds1, ds2, on=[:id1, :id2]) ≅
#           Dataset(a="a", id2=-(1:10^6), b="b", id1=1:10^6, c="c", d="d")
#     @test antijoin(ds1, ds2, on=[:id1, :id2]) ≅
#           Dataset(a="a", id2=-(10^7+1:10^7+2), b="b", id1=(10^7+1:10^7+2),
#                     c="c", d="d")
# end
#
# @testset "matchmissing :notequal correctness" begin
#     Random.seed!(1337)
#     names = [
#         Dataset(ID=[1, 2, missing],
#                   Name=["John Doe", "Jane Doe", "Joe Blogs"]),
#         Dataset(ID=[],
#                   Name=[]),
#         Dataset(ID=missings(3),
#                   Name=["John Doe", "Jane Doe", "Joe Blogs"]),
#         Dataset(ID=[1, 2, 3],
#                   Name=[missing, "Jane Doe", missing]),
#         Dataset(ID=[1:100; missings(100)],
#                   Name=repeat(["Jane Doe"], 200)),
#         Dataset(ID=[missings(100); 1:100],
#                   Name=repeat(["Jane Doe"], 200)),
#         Dataset(ID=[1:50; missings(100); 51:100],
#                   Name=repeat(["Jane Doe"], 200)),
#         Dataset(ID=[1:64; missings(64); 129:200],
#                   Name=repeat(["Jane Doe"], 200)),
#         Dataset(ID=[1:63; missings(65); 129:200],
#                   Name=repeat(["Jane Doe"], 200)),
#         Dataset(ID=rand([1:1000; missing], 10000),
#                   Name=rand(["John Doe", "Jane Doe", "Joe Blogs", missing], 10000)),
#     ]
#     jobs = [
#         Dataset(ID=[1, 2, 2, 4],
#                   Job=["Lawyer", "Doctor", "Florist", "Farmer"]),
#         Dataset(ID=[missing, 2, 2, 4],
#                   Job=["Lawyer", "Doctor", "Florist", "Farmer"]),
#         Dataset(ID=[missing, 2, 2, 4],
#                   Job=["Lawyer", "Doctor", missing, "Farmer"]),
#         Dataset(ID=[],
#                   Job=[]),
#         Dataset(ID=[1:100; missings(100)],
#                   Job=repeat(["Lawyer"], 200)),
#         Dataset(ID=[missings(100); 1:100],
#                   Job=repeat(["Lawyer"], 200)),
#         Dataset(ID=[1:50; missings(100); 51:100],
#                   Job=repeat(["Lawyer"], 200)),
#         Dataset(ID=[1:64; missings(64); 129:200],
#                   Job=repeat(["Lawyer"], 200)),
#         Dataset(ID=[1:63; missings(65); 129:200],
#                   Job=repeat(["Lawyer"], 200)),
#         Dataset(ID=rand([1:1000; missing], 10000),
#                   Job=rand(["Lawyer", "Doctor", "Florist", missing], 10000)),
#     ]
#     for name in names, job in jobs
#         @test leftjoin(name, dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
#             leftjoin(name, job, on=:ID, matchmissing=:notequal)
#         @test semijoin(name, dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
#             semijoin(name, job, on=:ID, matchmissing=:notequal)
#         @test antijoin(name, dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
#             antijoin(name, job, on=:ID, matchmissing=:notequal)
#         @test rightjoin(dropmissing(name, :ID), job, on=:ID, matchmissing=:equal) ≅
#             rightjoin(name, job, on=:ID, matchmissing=:notequal)
#         @test innerjoin(dropmissing(name, :ID), dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
#             innerjoin(name, job, on=:ID, matchmissing=:notequal)
#     end
#
#     rl(n) = rand(["a", "b", "c"], n)
#     names2 = [
#         Dataset(ID1=[1, 1, 2],
#                   ID2=["a", "b", "a"],
#                   Name=["John Doe", "Jane Doe", "Joe Blogs"]),
#         Dataset(ID1=[1, 1, 2, missing],
#                   ID2=["a", "b", "a", missing],
#                   Name=["John Doe", "Jane Doe", "Joe Blogs", missing]),
#         Dataset(ID1=[missing, 1, 2, missing],
#                   ID2=["a", "b", missing, missing],
#                   Name=[missing, "Jane Doe", "Joe Blogs", missing]),
#         Dataset(ID1=[missing, 1, 2, missing],
#                   ID2=["a", "b", missing, missing],
#                   Name=missings(4)),
#         Dataset(ID1=[missing, 1, 2, missing],
#                   ID2=missings(4),
#                   Name=["John Doe", "Jane Doe", "Joe Blogs", missing]),
#         Dataset(ID1=[1:100; missings(100)],
#                   ID2=[rl(100); missings(100)],
#                   Name=rand(["Jane Doe", "Jane Doe"], 200)),
#         Dataset(ID1=[missings(100); 1:100],
#                   ID2=[missings(100); rl(100)],
#                   Name=rand(["Jane Doe", "Jane Doe"], 200)),
#         Dataset(ID1=[1:50; missings(100); 51:100],
#                   ID2=[rl(50); missings(100); rl(50)],
#                   Name=rand(["Jane Doe", "Jane Doe"], 200)),
#         Dataset(ID1=[1:64; missings(64); 129:200],
#                   ID2=[rl(64); missings(64); rl(200 - 128)],
#                   Name=rand(["Jane Doe", "Jane Doe"], 200)),
#         Dataset(ID1=[1:63; missings(65); 129:200],
#                   ID2=[rl(64); missings(65); rl(200 - 129)],
#                   Name=rand(["Jane Doe", "Jane Doe"], 200)),
#         Dataset(ID1=rand([1:100; missing], 10000),
#                   ID2=rand(["a", "b", "c", missing], 10000),
#                   Name=rand(["John Doe", "Jane Doe", "Joe Blogs", missing], 10000)),
#     ]
#     jobs2 = [
#         Dataset(ID1=[1, 2, 2, 4],
#                   ID2=["a", "b", "b", "c"],
#                   Job=["Lawyer", "Doctor", "Florist", "Farmer"]),
#         Dataset(ID1=[1, 2, 2, 4, missing],
#                   ID2=["a", "b", "b", "c", missing],
#                   Job=["Lawyer", "Doctor", "Florist", "Farmer", missing]),
#         Dataset(ID1=[1, 2, missing, 4, missing],
#                   ID2=["a", "b", missing, "c", missing],
#                   Job=[missing, "Doctor", "Florist", "Farmer", missing]),
#         Dataset(ID1=[1:100; missings(100)],
#                   ID2=[rl(100); missings(100)],
#                   Job=rand(["Doctor", "Florist"], 200)),
#         Dataset(ID1=[missings(100); 1:100],
#                   ID2=[missings(100); rl(100)],
#                   Job=rand(["Doctor", "Florist"], 200)),
#         Dataset(ID1=[1:50; missings(100); 51:100],
#                   ID2=[rl(50); missings(100); rl(50)],
#                   Job=rand(["Doctor", "Florist"], 200)),
#         Dataset(ID1=[1:64; missings(64); 129:200],
#                   ID2=[rl(64); missings(64); rl(200 - 128)],
#                   Job=rand(["Doctor", "Florist"], 200)),
#         Dataset(ID1=[1:63; missings(65); 129:200],
#                   ID2=[rl(64); missings(65); rl(200 - 129)],
#                   Job=rand(["Doctor", "Florist"], 200)),
#         Dataset(ID1=rand([1:100; missing], 10000),
#                   ID2=rand(["a", "b", "c", missing], 10000),
#                   Job=rand(["Doctor", "Florist", "Farmer", missing], 10000)),
#     ]
#     k = [:ID1, :ID2]
#     for name in names2, job in jobs2
#         @test leftjoin(name, dropmissing(job, k), on=k, matchmissing=:equal) ≅
#             leftjoin(name, job, on=k, matchmissing=:notequal)
#         @test semijoin(name, dropmissing(job, k), on=k, matchmissing=:equal) ≅
#             semijoin(name, job, on=k, matchmissing=:notequal)
#         @test antijoin(name, dropmissing(job, k), on=k, matchmissing=:equal) ≅
#             antijoin(name, job, on=k, matchmissing=:notequal)
#         @test rightjoin(dropmissing(name, k), job, on=k, matchmissing=:equal) ≅
#             rightjoin(name, job, on=k, matchmissing=:notequal)
#         @test innerjoin(dropmissing(name, k), dropmissing(job, k), on=k, matchmissing=:equal) ≅
#             innerjoin(name, job, on=k, matchmissing=:notequal)
#     end
# end
#
# end # module
