
COMPONENT=hsn2-data-store

all:	${COMPONENT}-package

clean:	${COMPONENT}-package-clean

${COMPONENT}-package:
	mvn clean install -U -Pbundle
	mkdir -p build/data-store
	tar xzf target/${COMPONENT}-2.0.0-SNAPSHOT.tar.gz -C build/data-store

${COMPONENT}-package-clean:
	rm -rf build

build-local:
	mvn clean install -U -Pbundle
