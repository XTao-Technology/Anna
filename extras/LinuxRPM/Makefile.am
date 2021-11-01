

.PHONY: all

all:
	@echo "To build RPMS run 'make cephrpms'"

.PHONY: cephrpms prep srcrpm clean

cephrpms: prep rpms
if WITH_CLEAN_BUILD
prep:
	-mkdir -p rpmbuild/SPECS
	-mkdir -p rpmbuild/RPMS
	-mkdir -p rpmbuild/SRPMS
	-mkdir -p rpmbuild/SOURCES
	-mkdir -p rpmbuild/BUILD
	-mkdir -p rpmbuild/BUILDROOT

	cp ../../ceph.spec ./rpmbuild/SPECS
	$(MAKE) -C ../.. dist-bzip2
	cp ../../ceph-*.tar.bz2 ./rpmbuild/SOURCES/
endif

rpms:
if WITH_CLEAN_BUILD
	rpmbuild --define '_topdir $(shell pwd)/rpmbuild' -ba rpmbuild/SPECS/ceph.spec --with clean_build
else
	rpmbuild --define '_topdir $(shell pwd)/rpmbuild' -ba rpmbuild/SPECS/ceph.spec
endif
	mv rpmbuild/RPMS/*/* .
	mv rpmbuild/SRPMS/* .

clean:
	-rm -rf rpmbuild
	-rm -f *.rpm
