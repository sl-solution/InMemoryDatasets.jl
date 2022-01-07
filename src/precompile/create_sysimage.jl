function create_sysimage(sysout::AbstractString, add_dlmreader::Bool = true; add_package::Bool = true)
    juliaCMD = Base.julia_cmd()
    tmpIMD = mktemp()
    close(tmpIMD[2])
    if add_package
        run(`$juliaCMD -e "using Pkg; Pkg.add(\"PackageCompiler\")"`)
        if add_dlmreader
            run(`$juliaCMD -e "using Pkg; Pkg.add(\"DLMReader\")"`)
        end
    end
    warmupcode = "using InMemoryDatasets; ds = IMD.warmup();println(ds)"
    if add_dlmreader
        tmpDLM = mktemp()
        close(tmpDLM[2])
        warmupcode *= "; using DLMReader; repeat!(ds, 10000); ds.x1 = rand(10000); ds.x2 = rand(Int, 10000); filewriter(\"$(tmpDLM[1])\", ds); filereader(\"$(tmpDLM[1])\");"
    end

    run(`$juliaCMD --trace-compile=$(tmpIMD[1]) -e "$warmupcode"`)
    tmpIMD_out = mktemp()
    close(tmpIMD_out[2])
    f = open(tmpIMD[1])
    fout = open(tmpIMD_out[1], "w")
    # TODO should make this part less hard coding
    write(fout, "using REPL, Pkg, InMemoryDatasets, InMemoryDatasets.PooledArrays, InMemoryDatasets.DataAPI, InMemoryDatasets.PrettyTables, InMemoryDatasets.Tables, PackageCompiler, TOML, Logging, SuiteSparse
    ")
    if add_dlmreader
        write(fout, "; using DLMReader, DLMReader.InlineStrings.Parsers
        ")
    end
    for line in eachline(f)
        # not sure why this happens
        write(fout, replace(line, "000000"=>""))
    end
    close(fout)
    close(f)
    run(`$juliaCMD -e "using PackageCompiler;
    create_sysimage(:InMemoryDatasets, sysimage_path=\"$(sysout)\", precompile_execution_file=\"$(tmpIMD_out[1])\")"`)
    println("Now exit julia and re-run it again using $(sysout) as `--sysimage`, e.g. in unix type OS you can use the following command:
    julia --sysimage $sysout")
end
