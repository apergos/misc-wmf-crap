VERSION        = "0.0.1"
CC            ?= gcc
BIGFILES       = -D_FILE_OFFSET_BITS=64
CPPFLAGS      += $(BIGFILES) -DVERSION=\"$(VERSION)\"
CFLAGS        ?= -Wall -Werror -O2

build: revsperpage

NAME_REVSPERPAGE = "Display revisions per page from a MediaWiki xml dump"

PREFIX        ?= "/usr/local"
BINDIR         = $(DESTDIR)$(PREFIX)/bin/
DOCDIR         = $(DESTDIR)$(PREFIX)/share/doc/revsperpage/

SHELL          = /bin/sh

revsperpage: revsperpage.o
	$(CC) $(LDFLAGS) -o revsperpage revsperpage.o

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

install: revsperpage
	install --directory                             $(BINDIR)
	install --mode=755   revsperpage                $(BINDIR)
	install --directory                             $(DOCDIR)
	install --mode=644   README                     $(DOCDIR)
	install --mode=644   COPYING                    $(DOCDIR)

uninstall:
	rm -f $(BINDIR)revsperpage
	rm -f $(DOCDIR)README
	rm -f $(DOCDIR)COPYING

clean:
	rm -f revsperpage.o revsperpage
