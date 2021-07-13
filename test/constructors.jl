module TestConstructors

using Test, InMemoryDatasets, Random, CategoricalArrays, DataStructures, PooledArrays
using InMemoryDatasets: Index, _columns, index
const ≅ = isequal

#
# Dataset
#
@testset "constructors" begin
    ds = Dataset()
    @inferred Dataset()

    @test isempty(_columns(ds))
    @test _columns(ds) isa Vector{AbstractVector}
    @test index(ds) == Index()
    @test size(Dataset(copycols=false)) == (0, 0)

    vecvec = [CategoricalVector{Union{Float64, Missing}}(zeros(3)),
              CategoricalVector{Union{Float64, Missing}}(ones(3))]

    ds = Dataset(collect(Any, vecvec), Index([:x1, :x2]))
    @test size(ds, 1) == 3
    @test size(ds, 2) == 2

    ds2 = Dataset(collect(Any, vecvec), Index([:x1, :x2]), copycols=false)
    @test size(ds2, 1) == 3
    @test size(ds2, 2) == 2
    @test ds2.x1.val === vecvec[1]
    @test ds2.x2.val === vecvec[2]

    @test_throws ArgumentError Dataset([[1, 2]], :autos)
    @test_throws ArgumentError Dataset([1 2], :autos)

    for copycolsarg in (true, false)
        @test ds == Dataset(vecvec, :auto, copycols=copycolsarg)
        @test ds == Dataset(collect(Any, vecvec), :auto, copycols=copycolsarg)
        @test ds == Dataset(collect(AbstractVector, vecvec), :auto, copycols=copycolsarg)
        @test ds == Dataset(x1 = vecvec[1], x2 = vecvec[2], copycols=copycolsarg)

        for cols in ([:x1, :x2], ["x1", "x2"])
            @test ds == Dataset(vecvec, cols, copycols=copycolsarg)
            @test ds == Dataset(collect(Any, vecvec), cols, copycols=copycolsarg)
            @test ds == Dataset(collect(AbstractVector, vecvec), cols, copycols=copycolsarg)
            @test ds == Dataset([col=>vect for (col, vect) in zip(cols, vecvec)], copycols=copycolsarg)
        end
    end

    @test Dataset([1:3, 1:3], :auto) == Dataset(Any[1:3, 1:3], :auto) ==
          Dataset(UnitRange[1:3, 1:3], :auto) == Dataset(AbstractVector[1:3, 1:3], :auto) ==
          Dataset([[1, 2, 3], [1, 2, 3]], :auto) == Dataset(Any[[1, 2, 3], [1, 2, 3]], :auto) ==
          Dataset([1:3, [1, 2, 3]], :auto)
          Dataset([:x1=>1:3, :x2=>[1, 2, 3]]) == Dataset(["x1"=>1:3, "x2"=>[1, 2, 3]])

    @inferred Dataset([1:3, 1:3], :auto)
    @inferred Dataset([1:3, 1:3], [:a, :b])
    @inferred Dataset([1:3, 1:3], ["a", "b"])

    @inferred Dataset([:x1=>1:3, :x2=>[1, 2, 3]])
    @inferred Dataset(["x1"=>1:3, "x2"=>[1, 2, 3]])

    @test ds !== Dataset(ds)
    @test ds == Dataset(ds)

    @test ds == Dataset(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0])
    @test ds == Dataset(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0],
                          x3 = Union{Float64, Missing}[2.0, 2.0, 2.0])[:, [:x1, :x2]]
    @test ds == Dataset(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0],
                          x3 = Union{Float64, Missing}[2.0, 2.0, 2.0])[:, ["x1", "x2"]]

    @test_throws BoundsError SubDataset(Dataset(A=1), 0:0, :)
    @test_throws ArgumentError SubDataset(Dataset(A=1), 0, :)
    @test_throws BoundsError Dataset(A=1)[0, :]
    @test_throws BoundsError Dataset(A=1)[0, 1:1]

    @test Dataset(a=1, b=1:2) == Dataset(a=[1, 1], b=[1, 2])

    @test_throws ArgumentError Dataset(makeunique=true)
    @test_throws ArgumentError Dataset(a=1, makeunique=true)
    @test_throws ArgumentError Dataset(a=1, makeunique=true, copycols=false)
end

@testset "Dataset keyword argument constructor" begin
    x = allowmissing([1, 2, 3])
    y = allowmissing([4, 5, 6])

    ds = Dataset(x=x, y=y)
    @test size(ds) == (3, 2)
    @test propertynames(ds) == [:x, :y]
    @test ds.x == x
    @test ds.y == y
    @test ds.x.val !== x
    @test ds.y.val !== y
    ds = Dataset(x=x, y=y, copycols=true)
    @test size(ds) == (3, 2)
    @test propertynames(ds) == [:x, :y]
    @test ds.x == x
    @test ds.y == y
    @test ds.x.val !== x
    @test ds.y.val !== y
    ds = Dataset(x=x, y=y, copycols=false)
    @test size(ds) == (3, 2)
    @test propertynames(ds) == [:x, :y]
    @test ds.x.val === x
    @test ds.y.val === y
    @test_throws ArgumentError Dataset(x=x, y=y, copycols=1)

    ds = Dataset(x=x, y=y, copycols=false)
    @test size(ds) == (3, 2)
    @test propertynames(ds) == [:x, :y]
    @test ds.x.val === x
    @test ds.y.val === y
end

@testset "Dataset constructor" begin
    ds1 = Dataset(x=1:3, y=1:3)

    ds2 = Dataset(ds1)
    ds3 = copy(ds1)
    @test ds1 == ds2 == ds3
    @test ds1.x.val !== ds2.x.val
    @test ds1.x.val !== ds3.x.val
    @test ds1.y.val !== ds2.y.val
    @test ds1.y.val !== ds3.y.val

    ds2 = Dataset(ds1, copycols=false)
    ds3 = copy(ds1, copycols=false)
    @test ds1 == ds2 == ds3
    @test ds1.x.val === ds2.x.val
    @test ds1.x.val === ds3.x.val
    @test ds1.y.val === ds2.y.val
    @test ds1.y.val === ds3.y.val

    ds1 = view(ds1, :, :)
    ds2 = Dataset(ds1)
    ds3 = copy(ds1)
    @test ds1 == ds2 == ds3
    @test ds1.x.val !== ds2.x.val
    @test ds1.x.val !== ds3.x.val
    @test ds1.y.val !== ds2.y.val
    @test ds1.y.val !== ds3.y.val

    # FIXME
    # ds2 = Dataset(ds1, copycols=false)
    # @test ds1 == ds2
    # @test ds1.x.val === ds2.x.val
    # @test ds1.y.val === ds2.y.val
end

@testset "pair constructor" begin
    @test Dataset(:x1 => zeros(3), :x2 => ones(3)) ==
          Dataset([:x1 => zeros(3), :x2 => ones(3)]) ==
          Dataset("x1" => zeros(3), "x2" => ones(3)) ==
          Dataset("x1" => zeros(3), "x2" => ones(3))

    @inferred Dataset(:x1 => zeros(3), :x2 => ones(3))
    ds = Dataset([:x1 => zeros(3), :x2 => ones(3)])
    @test size(ds, 1) == 3
    @test size(ds, 2) == 2
    @test isequal(ds, Dataset(x1 = [0.0, 0.0, 0.0], x2 = [1.0, 1.0, 1.0]))

    ds = Dataset(:type => [], :begin => [])
    @test propertynames(ds) == [:type, :begin]

    a=allowmissing([1, 2, 3])
    ds = Dataset(:a=>a, :b=>1, :c=>1:3)
    @test propertynames(ds) == [:a, :b, :c]
    @test ds.a.val == a
    @test ds.a.val !== a

    ds = Dataset(:a=>a, :b=>1, :c=>1:3, copycols=false)
    @test propertynames(ds) == [:a, :b, :c]
    @test ds.a.val === a

    ds = Dataset("x1" => zeros(3), "x2" => ones(3))
    @inferred Dataset("x1" => zeros(3), "x2" => ones(3))
    @test size(ds, 1) == 3
    @test size(ds, 2) == 2
    @test isequal(ds, Dataset(x1 = [0.0, 0.0, 0.0], x2 = [1.0, 1.0, 1.0]))

    ds = Dataset("type" => [], "begin" => [])
    @test propertynames(ds) == [:type, :begin]

    a=allowmissing([1, 2, 3])
    ds = Dataset("a"=>a, "b"=>1, "c"=>1:3)
    @test propertynames(ds) == [:a, :b, :c]
    @test ds."a" == a
    @test ds."a".val !== a

    ds = Dataset("a"=>a, "b"=>1, "c"=>1:3, copycols=false)
    @test propertynames(ds) == [:a, :b, :c]
    @test ds."a".val === a

    @test_throws ArgumentError Dataset(["type" => 1, :begin => 2])
end

@testset "associative" begin
    ds = Dataset(Dict(:A => 1:3, :B => 4:6))
    @inferred Dataset(Dict(:A => 1:3, :B => 4:6))
    @test ds == Dataset(A = 1:3, B = 4:6)
    @test eltype.(eachcol(ds)) == Union[Union{Missing, Int}, Union{Missing, Int}]

    a=allowmissing([1, 2, 3])
    ds = Dataset(Dict(:a=>a, :b=>1, :c=>1:3))
    @test propertynames(ds) == [:a, :b, :c]
    @test ds.a == a
    @test ds.a.val !== a

    ds = Dataset(Dict(:a=>a, :b=>1, :c=>1:3), copycols=false)
    @test propertynames(ds) == [:a, :b, :c]
    @test ds.a.val === a

    ds = Dataset(Dict("A" => 1:3, "B" => 4:6))
    @inferred Dataset(Dict("A" => 1:3, "B" => 4:6))
    @test ds == Dataset(A = 1:3, B = 4:6)
    @test eltype.(eachcol(ds)) == Union[Union{Missing, Int}, Union{Missing, Int}]

    a=allowmissing([1, 2, 3])
    ds = Dataset(Dict("a"=>a, "b"=>1, "c"=>1:3))
    @test propertynames(ds) == [:a, :b, :c]
    @test ds."a" == a
    @test ds."a".val !== a
    ds = Dataset(Dict("a"=>a, "b"=>1, "c"=>1:3), copycols=false)
    @test propertynames(ds) == [:a, :b, :c]
    @test ds."a".val === a
end

@testset "vector constructors" begin
    x = allowmissing([1, 2, 3])
    y = allowmissing([1, 2, 3])

    ds = Dataset([x, y], :auto)
    @test propertynames(ds) == [:x1, :x2]
    @test ds.x1 == x
    @test ds.x2 == y
    @test ds.x1.val !== x
    @test ds.x2.val !== y
    ds = Dataset([x, y], :auto, copycols=true)
    @test propertynames(ds) == [:x1, :x2]
    @test ds.x1 == x
    @test ds.x2 == y
    @test ds.x1.val !== x
    @test ds.x2.val !== y
    ds = Dataset([x, y], :auto, copycols=false)
    @test propertynames(ds) == [:x1, :x2]
    @test ds.x1.val === x
    @test ds.x2.val === y

    ds = Dataset([x, y], [:x1, :x2])
    @test propertynames(ds) == [:x1, :x2]
    @test ds.x1 == x
    @test ds.x2 == y
    @test ds.x1.val !== x
    @test ds.x2.val !== y
    ds = Dataset([x, y], [:x1, :x2], copycols=true)
    @test propertynames(ds) == [:x1, :x2]
    @test ds.x1 == x
    @test ds.x2 == y
    @test ds.x1.val !== x
    @test ds.x2.val !== y
    ds = Dataset([x, y], [:x1, :x2], copycols=false)
    @test propertynames(ds) == [:x1, :x2]
    @test ds.x1.val === x
    @test ds.x2.val === y

    ds = Dataset([x, y], ["x1", "x2"])
    @test names(ds) == ["x1", "x2"]
    @test ds."x1" == x
    @test ds."x2" == y
    @test ds."x1".val !== x
    @test ds."x2".val !== y
    ds = Dataset([x, y], ["x1", "x2"], copycols=true)
    @test names(ds) == ["x1", "x2"]
    @test ds."x1" == x
    @test ds."x2" == y
    @test ds."x1".val !== x
    @test ds."x2".val !== y
    ds = Dataset([x, y], ["x1", "x2"], copycols=false)
    @test names(ds) == ["x1", "x2"]
    @test ds."x1".val === x
    @test ds."x2".val === y

    n = [:x1, :x2]
    v = AbstractVector[1:3, [1, 2, 3]]
    @test Dataset(v, n).x1.val isa Vector{Union{Int, Missing}}
    @test v[1] isa AbstractRange

    n = ["x1", "x2"]
    v = AbstractVector[1:3, [1, 2, 3]]
    @test Dataset(v, n)."x1".val isa Vector{Union{Int, Missing}}
    @test v[1] isa AbstractRange
end

@testset "recyclers" begin
    @test Dataset(a = 1:5, b = 1) == Dataset(a = collect(1:5), b = fill(1, 5))
    @test Dataset(a = 1, b = 1:5) == Dataset(a = fill(1, 5), b = collect(1:5))
    @test size(Dataset(a=1, b=[])) == (0, 2)
    @test size(Dataset(a=1, b=[], copycols=false)) == (0, 2)
end

@testset "constructor thrown exceptions" begin
    for copycolsarg in (true, false)
        @test_throws DimensionMismatch Dataset(Any[collect(1:10)], InMemoryDatasets.Index([:A, :B]), copycols=copycolsarg)
        @test_throws ArgumentError Dataset(A = rand(2, 2), copycols=copycolsarg)
        @test_throws ArgumentError Dataset(A = rand(2, 1), copycols=copycolsarg)
        @test_throws ArgumentError Dataset([1, 2, 3], :auto, copycols=copycolsarg)
        @test_throws DimensionMismatch Dataset(AbstractVector[1:3, [1, 2]], :auto, copycols=copycolsarg)
        @test_throws ArgumentError Dataset([1:3, 1], [:x1, :x2], copycols=copycolsarg)
        @test_throws ArgumentError Dataset([1:3, 1], ["x1", "x2"], copycols=copycolsarg)
        @test_throws ErrorException Dataset([1:3, 1], copycols=copycolsarg)
    end

    @test_throws MethodError Dataset([1 2; 3 4], :auto, copycols=false)
end

@testset "column types" begin
    ds = Dataset(A = 1:3, B = 2:4, C = 3:5)
    answer = [Array{Union{Missing, Int}, 1}, Array{Union{Missing, Int}, 1}, Array{Union{Missing, Int}, 1}]
    @test typeof.(eachcol(ds)) == answer
    ds[!, :D] = [4, 5, missing]
    push!(answer, Vector{Union{Int, Missing}})
    @test typeof.(eachcol(ds)) == answer
    ds[!, :E] .= 'c'
    push!(answer, Vector{Union{Missing, Char}})
    @test typeof.(eachcol(ds)) == answer
end

@testset "expansion of Ref and 0-dimensional arrays" begin
    @test Dataset(a=Ref(1), b=fill(1)) == Dataset(a=[1], b=[1])
    @test Dataset(a=Ref(1), b=fill(1), c=1:3) ==
          Dataset(a=[1, 1, 1], b=[1, 1, 1], c=1:3)
end

@testset "broadcasting into 0 rows" begin
    for ds in [Dataset(x1=1:0, x2=1), Dataset(x1=1, x2=1:0)]
        @test size(ds) == (0, 2)
        @test ds.x1.val isa Vector{Union{Missing, Int}}
        @test ds.x2.val isa Vector{Union{Missing, Int}}
    end
end

@testset "Dict constructor corner case" begin
    @test_throws ArgumentError Dataset(Dict('a' => 1, true => 2))
    @test_throws ArgumentError Dataset(Dict(:z => 1, "true" => 2))
    @test Dataset(Dict("z" => 1, "true" => 2)) == Dataset("true" => 2, "z" => 1)
    @test Dataset(Dict([Symbol(c) => i for (i, c) in enumerate('a':'z')])) ==
          Dataset(Dict([string(c) => i for (i, c) in enumerate('a':'z')])) ==
          Dataset([Symbol(c) => i for (i, c) in enumerate('a':'z')])
    @test Dataset(OrderedDict(:z => 1, :a => 2)) == Dataset(z=1, a=2)

end

@testset "removed constructors" begin
    @test_throws ArgumentError Dataset([1 2; 3 4])
    @test_throws ArgumentError Dataset([[1, 2], [3, 4]])
    @test_throws ArgumentError Dataset([Int, Float64], [:a, :b])
    @test_throws ArgumentError Dataset([Int, Float64], [:a, :b], 2)
    @test_throws ArgumentError Dataset([Int, Float64], ["a", "b"])
    @test_throws ArgumentError Dataset([Int, Float64], ["a", "b"], 2)
end

@testset "threading correctness tests" begin
    for x in (10, 2*10^6), y in 1:4
        ds = Dataset(rand(x, y), :auto)
        InMemoryDatasets.insertcols!(ds, 1, :g => PooledArray(rand(1:10, nrow(ds))))
        InMemoryDatasets.insertcols!(ds, 1, :g2 => CategoricalArray(rand(1:10, nrow(ds))))
        @test ds == copy(ds)
        @test ds == ds[1:nrow(ds), :]
        p = shuffle(1:nrow(ds))
        @test ds[p, :] == ds[p, :]
    end
end

end # module
