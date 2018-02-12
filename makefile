GRADLE=gradlew $(JRTOPTS) $(DEPOPTS)
JRTOPTS=-Pjava7RTPath=C:/Apps/Java/jdk1.8.0_162/jre/lib/rt.jar
DEPOPTS=-ParceyeDependBase=C:/cygwin64/home/SBausch/.arceye/ -ParceyeDependPath=/build/libs/

.PHONY: all clean new
.SILENT:

all:
	$(GRADLE) assemble

clean:
	$(GRADLE) clean

new:
	$(GRADLE) clean assemble
