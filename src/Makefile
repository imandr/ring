LIBFILES = __init__.py link2.py uplink2.py downlink3.py transmission.py py3.py diaglink.py

LIBDIR = $(DSTROOT)/ring

build:	$(LIBDIR)
	echo Version=\"$(VERSION)\" > version.py
	cp version.py $(LIBFILES) $(LIBDIR)
	
$(LIBDIR):
	mkdir -p $@