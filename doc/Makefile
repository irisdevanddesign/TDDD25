names := assignment0 assignment1 assignment2 assignment3 assignment4 assignment5
names += lesson

pdfs := $(addsuffix .pdf,$(names))

all: ${pdfs}

%.pdf: %.tex $(shell find include -name '*.tex')
	latexmk -silent -pdf -pvc- $<

clean:
	rm -f *-blx.bib *.aux *.bbl *.blg *.fdb_latexmk *.fls *.log *.nav *.out *.snm *.toc *.xml

.PHONY: all clean
