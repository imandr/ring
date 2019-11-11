VERSION = 1.0
DSTROOT = $(HOME)/build/ring
TARDIR = /tmp/$(USER)
TARFILE = $(TARDIR)/ring_$(VERSION).tar

all:	clean build tarfile
	
build:	$(DSTROOT)
	cd src; make DSTROOT=$(DSTROOT) VERSION=$(VERSION) build
	cd test; make DSTROOT=$(DSTROOT) build
	cd cdict; make DSTROOT=$(DSTROOT) build
	
tarfile: $(TARDIR)
	cd $(DSTROOT); tar cf $(TARFILE) *
	
clean:
	rm -rf $(DSTROOT)
	
$(DSTROOT):
	mkdir -p $@

$(TARDIR):
	mkdir -p $@
	
