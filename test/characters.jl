using Test


@testset "characters construction" begin
    @test Characters{3, UInt16}("abα") == "abα"
    @test Characters{2}("abc") == "ab"
    @test length(Characters{12}("12     ")) == 2
    @test length(Characters{3}("helloworld")) == 3
    @test String(Characters{12}(" abc  ")) == " abc"
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
end
