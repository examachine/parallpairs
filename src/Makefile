NAME = textcat

allpairsobj = util.cmo types.cmo parallel.cmo dv.cmo dynarray.cmo \
dynarray1.cmo dynarrayPair.cmo sparseVector.cmo allPairs.cmo  \
score.cmo scoreAccum.cmo  
parallpairsobj = parAllPairs.cmo parAllPairsHoriz.cmo allPairs2D.cmo 
allpairsxobj = util.cmx types.cmx parallel.cmx dv.cmx dynarray.cmx \
dynarray1.cmx  dynarrayPair.cmx sparseVector.cmx allPairs.cmx \
score.cmx scoreAccum.cmx  
parallpairsxobj = parAllPairs.cmx parAllPairsHoriz.cmx allPairs2D.cmx 

OBJECTS = util.cmo types.cmo parallel.cmo dv.cmo dynarray.cmo \
dynarray1.cmo dynarrayPair.cmo sparseVector.cmo allPairs.cmo \
score.cmo scoreAccum.cmo parAllPairsVert.cmo parAllPairsHoriz.cmo \
allPairs2D.cmo  
XOBJECTS = util.cmx types.cmx parallel.cmx dv.cmx dynarray.cmx \
dynarray1.cmx dynarrayPair.cmx sparseVector.cmx allPairs.cmx \
score.cmx scoreAccum.cmx parAllPairsVert.cmx parAllPairsHoriz.cmx \
allPairs2D.cmx
ARCHIVE  = $(NAME).cma
XARCHIVE = $(NAME).cmxa

REQUIRES = ocamlgraph unix str threads bigarray gz mpi
PREDICATES =

OCAMLC   = ocamlfind ocamlc -cc mpicc -g -thread -I +threads
#-I +ocamlmpi 

OCAMLL   = $(OCAMLC) -package "$(REQUIRES)" -linkpkg
#-I +ocamlmpi mpi.cma

OCAMLOPTFLAGS := -cc mpicc -thread -I +threads -unsafe
# -I +ocamlmpi  mpi.cmxa

ifneq ($(PROFILE),)
OCAMLOPTFLAGS := $(OCAMLOPTFLAGS) -p 
endif
OCAMLOPT = ocamlfind ocamlopt $(OCAMLOPTFLAGS) 
OCAMLOPTL = $(OCAMLOPT) -package "$(REQUIRES)" -linkpkg 

OCAMLOPT = ocamlfind ocamlopt $(OCAMLOPTFLAGS) -thread

OCAMLDEP = ocamldep

EXECS = all-pairs all-pairs-dbg process-documents \
par-all-pairs-vert par-all-pairs-vert-dbg par-all-pairs-horiz \
par-all-pairs-horiz-dbg all-pairs-2d all-pairs-2d-dbg \
dt-to-dv dv-filter-lp dv-filter dv-stats \
test-util test-arithmetic test-types test-parallel test-parallel2 \
test-score test-dynarray
#part-dv-vert  part-dv-2d

.PHONY: all pkg optpkg

all: $(EXECS)

interpret: util.cmo distance.cmo distanceMatrix.cmo classFeatures.cmo \
category.cmo svmFeatures.cmo types.cmo dv.cmo dynArray.cmo sparseVector.cmo \
allPairs.cmo

.PHONY:	interpret

par-all-pairs-vert: $(XARCHIVE) par_all_pairs_vert.cmx
	$(OCAMLOPTL) $^ -o $@

par-all-pairs-vert-dbg: $(ARCHIVE) par_all_pairs_vert.cmo
	$(OCAMLL) $^ -o $@

par-all-pairs-horiz: $(XARCHIVE) par_all_pairs_horiz.cmx
	$(OCAMLOPTL) $^ -o $@

par-all-pairs-horiz-dbg: $(ARCHIVE) par_all_pairs_horiz.cmo
	$(OCAMLL) $^ -o $@

par-all-pairs-dbg: $(ARCHIVE) par_all_pairs.cmo
	$(OCAMLL) $^ -o $@

part-dv-vert: $(XARCHIVE) partition_dv_vertical.cmx
	$(OCAMLOPTL) $^ -o $@

all-pairs: $(allpairsxobj) all-pairs.cmx
	$(OCAMLOPTL) $^ -o $@

all-pairs-dbg: $(allpairsobj) all-pairs.cmo
	$(OCAMLL) $^ -o $@

all-pairs-2d: $(XARCHIVE) all_pairs_2d.cmx
	$(OCAMLOPTL) $^ -o $@

all-pairs-2d-dbg: $(ARCHIVE)
	$(OCAMLL) $^ -o $@

part-dv-2d: $(XARCHIVE)  part_dv_2d.cmx
	$(OCAMLOPTL) $^ -o $@

test-dynarray: $(ARCHIVE) test_dynarray.cmo
	$(OCAMLL)  $^ -o $@

test-parallel: $(ARCHIVE) test_parallel.cmo
	$(OCAMLL)  $^ -o $@

test-parallel2: $(XARCHIVE) test_parallel2.cmx
	$(OCAMLOPTL)  $^ -o $@

test-score: $(ARCHIVE) test_score.cmo
	$(OCAMLL)  $^ -o $@

test-arithmetic: $(XARCHIVE) test_arithmetic.cmx
	$(OCAMLOPTL) $^ -o $@

test-dv: types.cmx util.cmx dv.cmx test_dv.cmx
	$(OCAMLOPTL) $^ -o $@

test-types: types.cmo util.cmo test_types.cmo
	$(OCAMLL) $^ -o $@

test-sparsevector: types.cmo util.cmo dynarray.cmo sparseVector.cmo \
	 test_sparsevector.cmo
	$(OCAMLL) $^ -o $@

test-util: util.cmo test_util.cmo
	$(OCAMLL) $^ -o $@

dt-to-dv: $(XARCHIVE) dt2dv.cmx
	$(OCAMLOPTL) $^ -o $@

dv-filter: $(XARCHIVE) dv_filter.cmx
	$(OCAMLOPTL) $^ -o $@

dv-filter-lp: $(XARCHIVE) dv_filter_lp.cmx
	$(OCAMLOPTL) $^ -o $@

dv-stats: $(XARCHIVE) dv_stats.cmx
	$(OCAMLOPTL) $^ -o $@

class-features-dbg: $(ARCHIVE) class-features.cmo
	$(OCAMLL) $^ -o $@

all-class-features: $(XARCHIVE) all-class-features.cmx
	$(OCAMLOPTL) $^ -o $@

all-class-features-dbg: $(ARCHIVE) all-class-features.cmo

class-features: $(XARCHIVE) class-features.cmx
	$(OCAMLOPTL) $^ -o $@

make-svm-data: $(XARCHIVE) make-svm-data.cmx
	$(OCAMLOPTL) $^ -o $@

split-train-test: $(XARCHIVE) split-train-test.cmx
	$(OCAMLOPTL) $^ -o $@

merge-features: $(XARCHIVE) merge-features.cmx
	$(OCAMLOPTL) $^ -o $@

dv-to-svm: $(XARCHIVE) dv-to-svm.cmx
	$(OCAMLOPTL) $^ -o $@

process-documents: processDocuments.C
	g++ -g $^ -o $@

pkg: $(ARCHIVE)
optpkg: $(XARCHIVE)

$(ARCHIVE): $(OBJECTS)
	$(OCAMLC) -a -o $(ARCHIVE) -predicates "$(PREDICATES)" $(OBJECTS)
$(XARCHIVE): $(XOBJECTS)
	$(OCAMLOPT) -a -o $(XARCHIVE) $(XOBJECTS)

.SUFFIXES: .cmo .cmi .cmx .ml .mli

.ml.cmo:
	$(OCAMLC) -package "$(REQUIRES)" -predicates "$(PREDICATES)" \
	-c $<
.mli.cmi:
	$(OCAMLC) -package "$(REQUIRES)" -predicates "$(PREDICATES)" \
	-c $<
.ml.cmx:
	$(OCAMLOPT) -package "$(REQUIRES)" -predicates "$(PREDICATES)" \
	-c $<

include depend

depend: $(wildcard *.ml*)
	if ! ($(OCAMLDEP) *.mli *.ml >depend); then rm depend; fi

.PHONY: install uninstall clean

install: all
	{ test ! -f $(XARCHIVE) || extra="$(XARCHIVE) "`basename $(XARCHIVE) .cmxa`.a }; \
	ocamlfind install $(NAME) *.mli *.cmi $(ARCHIVE) META $$extra

uninstall:
	ocamlfind remove $(NAME)

clean:
	rm -f depend *.cmi *.cmo *.cmx *.cma *.cmxa *.a $(EXECS)
	rm -f depend *.dvi *.log *.aux *.ps
