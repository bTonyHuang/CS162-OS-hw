/*

Copyright © 2019 University of California, Berkeley

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

word_count provides lists of words and associated count

Functional methods take the head of a list as first arg.
Mutators take a reference to a list as first arg.
*/

#include "word_count.h"

/* Basic utilities */

char *new_string(char *str) {
  char *new_str = (char *) malloc(strlen(str) + 1);
  if (new_str == NULL) {
    return NULL;
  }
  return strcpy(new_str, str);
}

int init_words(WordCount **wclist) {
  /* Initialize word count.
     Returns 0 if no errors are encountered
     in the body of this function; 1 otherwise.
  */

  /**wclist = (WordCount*)calloc(sizeof(WordCount),sizeof(char));
   if(!(*wclist)){
      printf("wclist calloc failed\n");
      return 1;
    }
  */
  return 0;
}

ssize_t len_words(WordCount *wchead) {
  /* Return -1 if any errors are
     encountered in the body of
     this function.
  */
  if(!wchead)
    return -1;
  size_t len = 0;
  WordCount *pointer=wchead;
  //no word only initial
  if(!pointer->count)
    return 0;
  while(pointer){
    len++;
    pointer=pointer->next;
  }
  return len;
}

WordCount *find_word(WordCount *wchead, char *word) {
  /* Return count for word, if it exists */
  WordCount *wc=NULL,*pointer=wchead;
  //if wchead or word is NULL then return NULL
  if(!word)
    return wc;
  while(pointer){
    if(!pointer->word)
      break;
    if(!strcmp(pointer->word,word)){
      wc=pointer;
      break;
    }
    pointer=pointer->next;
  }
  return wc;
}

int add_word(WordCount **wclist, char *word) {
  /* If word is present in word_counts list, increment the count.
     Otherwise insert with count 1.
     Returns 0 if no errors are encountered in the body of this function; 1 otherwise.
  */
  //param validation
  if(!wclist||!word){
    printf("add word is NULL\n");
    return 1;
  }

  WordCount* wc=find_word(*wclist,word);
  //presence, count++
  if(wc)
    wc->count++;
  //insert with count 1.
  else{
    wc=(WordCount*)calloc(sizeof(WordCount),sizeof(char));
    if(!wc){
      printf("calloc failed\n");
      return 1;
    }

    wc->count=1;

    wc->word=new_string(word);
    if(!wc->word){
      printf("new_string failed\n");
      return 1;
    }

    if(len_words(*wclist)<1){
      (*wclist)=wc;
      wc->next=NULL;
    }
    else{
      wc->next=(*wclist)->next;
    (*wclist)->next=wc;
    }
  }

 return 0;
}

void fprint_words(WordCount *wchead, FILE *ofile) {
  /* print word counts to a file */
  WordCount *wc;
  for (wc = wchead; wc; wc = wc->next) {
    fprintf(ofile, "%i\t%s\n", wc->count, wc->word);
  }
}
