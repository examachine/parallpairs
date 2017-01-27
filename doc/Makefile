LATEX=latex
BIBTEX=bibtex
PDFLATEX=pdflatex

default: all

texfiles = $(wildcard *.tex)

tables = $(wildcard tables/*.tex)

figures = $(wildcard figures/*.fig)
figuretargets = $(patsubst %.fig,%.ps,$(figures))

psfiles = $(wildcard *.eps) $(wildcard *.ps)

dotfiles = $(wildcard graphs/*.dot)
graphfiles = $(patsubst %.dot,%.ps,$(dotfiles))

all: parallpairs.pdf multilevelallpairs.pdf

*pdf: all-pairs.bib

aux: $(tables) $(figuretargets) $(psfiles) $(graphfiles)
 
%.ps: %.dvi
	dvips $< -o $@


%.pdf: %.dvi
	dvipdf $< $@

#%.pdf: %.tex
#	@echo Compiling latex to dvi $<
#	@$(PDFLATEX) $<
#	if [ -e $*.bib ]; then $(MAKE) $*.bbl; $(PDFLATEX) $<; fi
#	# ensure references are correct
#	@$(PDFLATEX) $<

%.dvi : %.tex
	@echo Compiling latex to dvi $<
	$(LATEX) $<
	$(BIBTEX) $*
	$(LATEX) $<
	# ensure references are correct
	$(LATEX) $<

# build rules for fig files

$(figuretargets): %.ps: %.fig
	@echo Generating figure $*
	fig2dev -L eps $< $@

# Pattern rules for dot/neato files
graphs/%.ps: graphs/%.dot
	dot -Tps $< -o $@
graphs/%.ps: graphs/%.neato
	neato -Tps $< -o $@

# A target to build code, who knows it might not be up-to-date!
code:
	@echo building code
	(cd $(CODE); $(MAKE))

OPTS = -long_titles 3 -dir handout -split 0 -html_version 4.0 \
 -address "Eray Ozkural <erayo@cs.bilkent.edu.tr>" \
 -auto_navigation \
 -antialias -antialias_text -white -no_reuse

handout:
	-mkdir handout
	latex2html $(OPTS) handout.tex


clean: 
	@echo Cleaning intermediate files
	@-rm *.dvi *.log *.aux *.bbl *.blg

distclean realclean: clean
	@echo Cleaning output and backup files
	@-rm *.ps *.pdf *~

recursive:
	list='$(SUBDIRS)'; for subdir in $$list; do \
	  echo "** Making $$target in $$subdir"; \
	  (cd $$subdir && $(MAKE) ) \
	done;

test:
	echo $(figuretargets)

.PHONY:	all aux recursive link code test handout

#.PRECIOUS: response.dvi noclique.dvi

# A pattern rule for eps files under figure
# size is the magnify ratio of the eps file
#figures/%.eps: figures/%.fig figures/%.fig.size
#%	fig2dev -L eps -m $(shell cat $<.size) $< $@
