using Test


@testset "characters construction" begin
    @test Characters{2}("abc") == "ab"
    @test length(Characters{12}("12     ")) == 2
    @test length(Characters{3}("helloworld")) == 3
    @test String(Characters{12}(" abc  ")) == " abc"
    @test isequal(Characters(""), missing)
    @test isequal(Characters{3}.(["a", "b", "", missing]), ["a","b", missing, missing])
end

@testset "characters comparison" begin
    @test c"12" == "12"
    @test c"12  " == "12"
    @test "ab cd e" == c"ab cd e"
    @test c"gh   " == c"gh"
    @test "12" < c"13"
    @test !isless(c"12  ", "12")
    @test !isless(c"12  ", c"12")
    @test isless("abc", c"xy    ")
    @test isless(c"abc", "x y z")
    @test isless(c"1", missing)
end
