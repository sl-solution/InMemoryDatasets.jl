function Base.show(io::IO, dsr::DatasetRow;
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   eltypes::Bool = true,
                   truncate::Int = 32,
                   kwargs...)
    r, c = parentindices(dsr)
    _show(io, view(parent(dsr), [r], c); allcols=allcols, rowlabel=rowlabel,
          summary=false, rowid=r, eltypes=eltypes, truncate=truncate,
          title="DatasetRow", kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", dsr::DatasetRow;
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(io, dsr; allcols=allcols, rowlabel=rowlabel, eltypes=eltypes,
         truncate=truncate, kwargs...)

Base.show(dsr::DatasetRow;
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(stdout, dsr; allcols=allcols, rowlabel=rowlabel, eltypes=eltypes,
         truncate=truncate, kwargs...)
