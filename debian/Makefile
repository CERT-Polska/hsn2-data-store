#
# This Makefile is responsoble for build debian package.
# 4 variables must be provided:
#   HSN2_VER=2.X
#   BUILD_NUMBER=999
#   DEBIAN_DIST=(experimental|2.X)

DEBIAN_DIST=experimental
HSN2_COMPONENT=data-store

all:	hsn2-data-store

clean:	hsn2-data-store-clean

PKG=hsn2-$(HSN2_COMPONENT)_$(HSN2_VER)-$(BUILD_NUMBER)_all
hsn2-data-store: hsn2-data-store-clean
	mkdir -p $(PKG)/DEBIAN
	mkdir -p $(PKG)/etc/init.d
	mkdir -p $(PKG)/opt/hsn2/$(HSN2_COMPONENT)
	tar -zxf ../target/hsn2-data-store-*.tar.gz -C $(PKG)/opt/hsn2/$(HSN2_COMPONENT)
	cp ds.initd $(PKG)/etc/init.d/hsn2-$(HSN2_COMPONENT)
	#no more required after change from redmine #6586
	#echo -n "1" > $(PKG)/opt/hsn2/$(HSN2_COMPONENT)/conf.conf
	cp control ${PKG}/DEBIAN
	sed -i "s/{VER}/${HSN2_VER}-${BUILD_NUMBER}/" $(PKG)/DEBIAN/control
	sed -i "s/{DEBIAN_DIST}/${DEBIAN_DIST}/" $(PKG)/DEBIAN/control
	fakeroot dpkg -b $(PKG)

hsn2-data-store-clean:
	rm -rf hsn2-data-store_*
