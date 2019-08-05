#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>

typedef enum { None, StartPage, Title, StartNS, PageId, StartRev, ByteLen, EndPage } States;

void usage(char *me) {
  fprintf(stderr,"Usage: %s [all] [bytes] [length] [maxrevlen] [title] <number>\n",me);
  fprintf(stderr,"counts number of revisions in each page\n");
  fprintf(stderr,"with 'all', displays the page id for each revision\n");
  fprintf(stderr,"for all namespaces\n");
  fprintf(stderr,"with 'length', displays the sum of byte lengths for\n");
  fprintf(stderr,"each page\n");
  fprintf(stderr,"with 'maxrevlen', displays the max byte length for\n");
  fprintf(stderr,"revisions of the page\n");
  fprintf(stderr,"with 'title', displays the title for each page\n");
  fprintf(stderr,"without 'all', displays only the revision count, and\n");
  fprintf(stderr,"only for the main namespace (ns 0)\n");
  fprintf(stderr,"with cutoff number, prints only information for pages\n");
  fprintf(stderr,"with more revisions than the cutoff\n");
}

/* note that even if we have only read a partial line
   of text from the body of the page, (cause the text
   is longer than our buffer), it's fine, since the
   <> delimiters only mark xml, they can't appear
   in the page text.

   returns new state */
States setState (char *line, States currentState) {
  if (!strncmp(line,"<page>",6)) {
    return(StartPage);
  }
  else if (!strncmp(line,"<title>",7)) {
    return(Title);
  }
  else if (currentState == Title && !strncmp(line, "<ns>", 4)) {
    return(StartNS);
  }
  else if (currentState == StartNS && !strncmp(line,"<id>",4)) {
    return(PageId);
  }
  else if (!strncmp(line,"<revision>",10)) {
    return(StartRev);
  }
  else if (!strncmp(line,"<text ",6)) {
    return(ByteLen);
  }
  else if (!strncmp(line, "</page>", 6)) {
      return(EndPage);
  }
  else if (!strncmp(line, "</mediawiki",11)) {
    return(None);
  }
  return(currentState);
}

int get_bytelen(char *text) {
  int length = 0;
  char *entry = NULL;

  /* typical entry in stubs used to be: <text id="11453" bytes="4837" /> */
  /* now: <text xml:space="preserve" bytes="141920" id="87207" /> */
  /* first byte */
  entry = strtok(text, "\"");
  if (entry == NULL)
    return(length);
  /* 'preserve' */
  entry = strtok(NULL, "\"");
  if (entry == NULL)
    return(length);
  /* 'bytes=' */
  entry = strtok(NULL, "\"");
  if (entry == NULL)
    return(length);
  /* byte length */
  entry = strtok(NULL, "\"");
  length = strtol(entry, NULL, 10);
  return(length);
}

int main(int argc,char **argv) {
  States state = None;
  char *text;
  char line[4097];
  int revisions;
  int length;
  int revlen;
  int maxrevlen;
  int good;
  char *datestring = NULL;
  int res=0;
  int all=0;
  int do_length=0;
  int do_title = 0;
  int pageid = 0;
  int cutoff = 0;
  int do_maxrevlen = 0;
  long long cumul = 0L;
  int i;
  char *title = NULL;

  if (argc < 1 || argc > 5) {
    fprintf(stderr, "missing args or too many args\n");
    usage(argv[0]);
    exit(-1);
  }
  if (argc > 1) {
    for (i=1; i< argc; i++) {
      if (!strncmp(argv[i],"all",3)) {
	all=1;
      }
      else if (!strncmp(argv[i],"bytes",5)) {
	do_length=1;
      }
      else if (!strncmp(argv[i],"title",5)) {
	do_title=1;
      }
      else if (!strncmp(argv[i],"maxrevlen",9)) {
	do_maxrevlen=1;
      }
      else if (isdigit(argv[i][0])) {
	cutoff = strtol(argv[i], NULL, 10);
      }
      else {
	fprintf(stderr, "unknown arg '%s'\n", argv[i]);
	usage(argv[0]);
	exit(-1);
      }
    }
  }
  while (fgets(line, sizeof(line)-1, stdin) != NULL) {
    text=line;
    while (*text && isspace(*text))
      text++;
    state = setState(text, state);
    if (state == StartPage) {
      revisions = 0;
      length = 0;
      maxrevlen = 0;
      good = 0;
      if (title != NULL)
	free(title);
    }
    if (state == StartNS) {
      if (!all && strncmp(text,"<ns>0</ns>",10)) {
	good = 0;
      }
      else {
	good = 1;
      }
    }
    if (state == ByteLen && good) {
      revlen = get_bytelen(text);
      if (revlen > maxrevlen)
        maxrevlen = revlen;
      length+= revlen;
      state = None;
    }
    if (state == PageId) {
      text+=4; /* skip <id> tag */
      pageid = strtol(text, NULL, 10);
      state = None;
    }
    if (state == Title) {
      text+=7; /* skip <title> tag */
      title = strndup(text, strlen(text) - 9);
    }
    if (state == StartRev && good) {
      revisions++;
      state = None;
    }
    if (state == EndPage) {
      if (revisions && revisions > cutoff) {
	if (all)
	  fprintf(stdout, "page:%d ",pageid);
	if (do_length)
	  fprintf(stdout, "bytes:%d ",length);
	if (do_maxrevlen)
	  fprintf(stdout, "maxrevlen:%d ",maxrevlen);
	fprintf(stdout, "revs:%d",revisions);
	if (do_title)
	  fprintf(stdout, " title:%s\n",title);
	else
	  fprintf(stdout, "\n");
      }
      state = None;
    }
  }
  exit(0);
}
