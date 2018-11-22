#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>
#include <ctype.h>

typedef enum { None, StartPage, StartNS, PageId, StartRev, EndPage } States;

void usage(char *me) {
  fprintf(stderr,"Usage: %s [all] <number>\n",me);
  fprintf(stderr,"counts number of revisions in each page\n");
  fprintf(stderr,"with 'all', displays the page id for each revision\n");
  fprintf(stderr,"for all namespaces\n");
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
  else if (currentState == StartPage && !strncmp(line, "<ns>", 4)) {
    return(StartNS);
  }
  else if (currentState == StartNS && !strncmp(line,"<id>",4)) {
    return(PageId);
  }
  else if (!strncmp(line,"<revision>",10)) {
    return(StartRev);
  }
  else if (!strncmp(line, "</page>", 6)) {
      return(EndPage);
  }
  else if (!strncmp(line, "</mediawiki",11)) {
    return(None);
  }
  return(currentState);
}


int main(int argc,char **argv) {
  States state = None;
  char *text;
  char line[4097];
  int revisions;
  int good;
  char *datestring = NULL;
  int res=0;
  int all=0;
  int pageid = 0;
  int cutoff = 0;
  long long cumul = 0L;

  if (argc < 1 || argc > 3) {
    usage(argv[0]);
    exit(-1);
  }
  if (argc > 1) {
    if (!strncmp(argv[1],"all",3)) {
      all=1;
    }
    else if (isdigit(argv[1][0])) {
      cutoff = strtol(text, NULL, 10);
    }
    else {
      usage(argv[0]);
      exit(-1);
    }
  }
  /* I'm lazy, I need to get this done in 2 seconds, not 5 minutes */
  if (argc > 2) {
    if (!strncmp(argv[2],"all",3)) {
      all=1;
    }
    else if (isdigit(argv[2][0])) {
      cutoff = strtol(argv[2], NULL, 10);
    }
    else {
      usage(argv[0]);
      exit(-1);
    }
  }
  while (fgets(line, sizeof(line)-1, stdin) != NULL) {
    text=line;
    while (*text && isspace(*text))
      text++;
    state = setState(text, state);
    if (state == StartPage) {
      revisions = 0;
      good = 0;
    }
    if (state == StartNS) {
      if (!all && strncmp(text,"<ns>0</ns>",10)) {
	good = 0;
      }
      else {
	good = 1;
      }
    }
    if (state == PageId) {
      text+=4; /* skip <id> tag */
      pageid = strtol(text, NULL, 10);
      state = None;
    }
    if (state == StartRev && good) {
      revisions++;
      state = None;
    }
    if (state == EndPage) {
      if (revisions && revisions > cutoff) {
	if (all)
	  fprintf(stdout, "%d %d\n",pageid, revisions);
	else
	  fprintf(stdout, "%d %d\n",pageid, revisions);
      }
      state = None;
    }
  }
  exit(0);
}
